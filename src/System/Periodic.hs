{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ScopedTypeVariables   #-}

{-|

This module contains a very basic periodic job processor backed by
Redis, suitable to be run by clusters of machines (jobs will be run at
least once per scheduled time, and will not run more than once unless
the (configurable) timeout is reached, at which point retry
occurs. There is also no guarantee that distribution of jobs across
machines will be fair). It depends upon Redis not losing data once it
has acknowledged it, and guaranteeing the atomicity that is specified
for commands like EVAL (ie, that if you do several things within an
EVAL, they will all happen or none will happen). Nothing has been
tested with Redis clusters (and it likely will not work).

An example use is the following (provided in repository), noting that
we create two threads to process jobs, and could run this entire
executable many times and the jobs would only run at the scheduled
time.

> {-# LANGUAGE OverloadedStrings #-}
> import           Control.Monad      (forever)
> import           System.Periodic
> import           Control.Concurrent      (threadDelay, forkIO)
> import qualified Data.Text.IO            as T
> import qualified Database.Redis          as R
> main = do rconn <- R.connect R.defaultConnectInfo
>           scheduler <- create "default" rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000)) (T.putStrLn)
>           addTask scheduler "print-hello-job" (Every (Seconds 100)) (T.putStrLn "hello")
>           addTask scheduler "print-bye-job" (Every (Seconds 10)) (T.putStrLn "bye")
>           forkIO (run scheduler)
>           forkIO (run scheduler)
>           forever (threadDelay 1000000)


-}

module System.Periodic
       ( -- * Types
         Time(..)
       , Seconds(..)
       , Period(..)
       , Name(..)
       , CheckInterval(..)
       , LockTimeout(..)
       , Logger
       , Scheduler
       -- * Managing Schedulers
       , create
       , run
       , destroy
       -- * Scheduling Tasks
       , addTask
       ) where

import           Control.Concurrent      (forkIO, threadDelay)
import           Control.Concurrent.MVar
import           Control.Exception       (SomeException, catch)
import           Control.Monad           (forever, join, when)
import           Data.Monoid
import           Data.Ratio
import           Data.Serialize
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import           Data.Time.Calendar
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import qualified Database.Redis          as R


-- | Time from midnight UTC
newtype Time = Time DiffTime

newtype Seconds = Seconds Int

-- | When the job should run - either at a particular offset from
-- midnight UTC, or every N seconds.
data Period = Daily Time | Every Seconds

-- | The name of the scheduler. This, when combined with the task
-- name, should be unique on a given Redis server.
newtype Name = Name Text

-- | How frequently we should check if jobs need to get run. If all
-- your jobs are infrequent (daily, or occurring hours apart), setting
-- this to a high number (every minute, or more) decreases the number
-- of queries to Redis.
newtype CheckInterval = CheckInterval Seconds

-- | How long a job that has been started and not marked as finished
-- should take before we run it again. Note that if you put this
-- number above the period, we will never retry the jobs (but they
-- will still keep running every period).
newtype LockTimeout = LockTimeout Seconds

-- | A function where log messages are sent.
type Logger = Text -> IO ()

-- | Internal type representing the current tasks that are scheduled.
data Scheduler = Scheduler { schedulerName          :: Name
                           , schedulerRedisConn     :: R.Connection
                           , schedulerTasks         :: MVar [(Text, Period, IO ())]
                           , schedulerCheckInterval :: CheckInterval
                           , schedulerLockTimout    :: LockTimeout
                           , schedulerLogger        :: Logger
                           }

-- | This function creates a new scheduler, which can then have tasks
-- scheduled in it. The tasks themselves are _not_ stored in Redis; it
-- is only used to ensure we run the tasks at the right time (and not
-- more than once across multiple machines). Note that this does not
-- actually run any of the tasks - you must fork threads that call
-- 'run'.
create :: Name
       -> R.Connection
       -> CheckInterval
       -> LockTimeout
       -> (Text -> IO ())
       -> IO Scheduler
create name rconn check lock logger =
  do mv <- newMVar []
     return $ Scheduler name rconn mv check lock logger

-- | Add a new task to a given scheduler, to be run at the specified
-- period. Note that the name, when combined with the scheduler name,
-- should be unique for the given redis database, or else only one of
-- the jobs with the same name will be run.
addTask :: Scheduler -> Text -> Period -> IO () -> IO ()
addTask scheduler name when act =
  do modifyMVar_ (schedulerTasks scheduler) (return . (:) (name, when, act))
     return ()

lockTimeout = 10 * 3600

-- | Start a scheduler thread. You must start at least one, but can
-- start multiple threads if you have many tasks that take a long time
-- to run.
run :: Scheduler -> IO ()
run (Scheduler (Name nm) rconn mv (CheckInterval (Seconds check)) lock logger) = forever $
    do now <- getCurrentTime
       tasks <- readMVar mv
       mapM_ (tryRunTask logger lock nm rconn now) tasks
       threadDelay (check * 1000000)

lastStartedKey pname name =
  T.encodeUtf8 $ pname <> "-" <> name <> "-last-started"
lockedKey pname name =
  T.encodeUtf8 $ pname <> "-" <> name <> "-running-at"

-- | This clears out redis of any mention of tasks related to the
-- scheduler. It isn't necessary to do in normal scenarios.
destroy :: Scheduler -> IO ()
destroy (Scheduler (Name nm) rconn mv _ _ _) =
  do tasks <- readMVar mv
     R.runRedis rconn $
       R.del (concatMap (\(tnm,_,_) ->
                            [lastStartedKey nm tnm
                            ,lockedKey nm tnm])
                        tasks)
     return ()

collapseError :: Either a (Maybe b) -> Maybe b
collapseError (Left _) = Nothing
collapseError (Right v) = v

collapseNumberBoolFalse :: Either R.Reply (Maybe Integer) -> Bool
collapseNumberBoolFalse (Left e) = error (show e)
collapseNumberBoolFalse (Right (Just 1)) = True
collapseNumberBoolFalse (Right (Just 0)) = False


parseUnixTime s = let Right (n, d) = decode s in posixSecondsToUTCTime $ fromRational $ n % d

renderUnixTime t = let r =  toRational . utcTimeToPOSIXSeconds $ t in encode (numerator r, denominator r)

minutes5 = 60 * 5

shouldRun :: LockTimeout -> Period -> Maybe UTCTime -> Maybe UTCTime -> UTCTime -> Bool
shouldRun (LockTimeout (Seconds timeout)) _ _ (Just locked) now
  = diffUTCTime now locked > fromIntegral timeout
shouldRun _ (Daily (Time t)) Nothing _ now
  | now > (now { utctDayTime = t }) && utctDayTime now - t < minutes5
  = True
shouldRun _ (Daily (Time t)) (Just last) _ now
  | now > (now { utctDayTime = t }) && last < (now { utctDayTime = t })
  = True
shouldRun _ (Every (Seconds n)) Nothing _ now
  = True
shouldRun _ (Every (Seconds n)) (Just last) locked now
  = diffUTCTime now last > fromIntegral n
shouldRun lock period last locked now = False


tryRunTask :: Logger -> LockTimeout -> Text -> R.Connection -> UTCTime -> (Text, Period, IO ()) -> IO ()
tryRunTask logger timeout pname rconn now (name, period, task) =
  do lastStarted <- fmap parseUnixTime . collapseError <$>
                    R.runRedis rconn
                               (R.get (lastStartedKey pname name))
     lockedAt <- fmap parseUnixTime . collapseError <$>
                 R.runRedis rconn
                            (R.get (lockedKey pname name))
     when (shouldRun timeout period lastStarted lockedAt now) $
       do gotLock <-
            collapseNumberBoolFalse <$> R.runRedis rconn
            (R.eval "local lock = redis.call('get', KEYS[1])\n\
                    \local last = redis.call('get', KEYS[2])\n\
                    \if ((lock == false and ARGV[1] == '0') or (lock == ARGV[1]))\
                    \  and ((last == false and ARGV[2] == '0') or (last == ARGV[2])) then\n\
                    \  redis.call('set', KEYS[1], ARGV[3])\n\
                    \  redis.call('set', KEYS[2], ARGV[3])\n\
                    \  return 1\n\
                    \else\n\
                    \  return 0\n\
                    \end" [lockedKey pname name, lastStartedKey pname name]
                          [maybe "0" renderUnixTime lockedAt
                          ,maybe "0" renderUnixTime lastStarted
                          ,renderUnixTime now])
          when gotLock $
            do x <- newEmptyMVar
               jt <- forkIO (catch (task >> putMVar x Nothing)
                                   (\(e::SomeException) ->
                                      putMVar x (Just e)))
               res <- takeMVar x
               case res of
                 Just e -> logger $ T.concat ["periodic["
                                             ,pname
                                             ,"::"
                                             ,name
                                             ,"] error: "
                                             ,T.pack (show e)]
                 Nothing -> return ()
               R.runRedis rconn $ R.del [lockedKey pname name]
               return ()
