{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PartialTypeSignatures #-}

module System.Periodic where

import           Control.Concurrent      (threadDelay)
import           Control.Concurrent.MVar
import           Control.Monad           (forever, join, when)
import           Data.Monoid
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import           Data.Time.Calendar
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import qualified Database.Redis          as R


newtype Time = Time DiffTime -- time from midnight UTC

newtype Seconds = Seconds Int

data Period = Daily Time | Every Seconds

newtype Name = Name Text
newtype CheckInterval = CheckInterval Seconds
newtype LockTimeout = LockTimeout Seconds

data Scheduler = Scheduler { schedulerName          :: Name
                           , schedulerRedisConn     :: R.Connection
                           , schedulerTasks         :: MVar [(Text, Period, IO ())]
                           , schedulerCheckInterval :: CheckInterval
                           , schedulerLockTimout    :: LockTimeout
                           }

create :: Name -> R.Connection -> CheckInterval -> LockTimeout -> IO Scheduler
create name rconn check lock = do mv <- newMVar []
                                  return $ Scheduler name rconn mv check lock

addTask :: Scheduler -> Text -> Period -> IO () -> IO ()
addTask scheduler job when act =
  do modifyMVar_ (schedulerTasks scheduler) (return . (:) (job, when, act))
     return ()

lockTimeout = 10 * 3600

run :: Scheduler -> IO ()
run (Scheduler (Name nm) rconn mv (CheckInterval (Seconds check)) lock) = forever $
    do now <- getCurrentTime
       tasks <- readMVar mv
       mapM_ (tryRunTask lock nm rconn now) tasks
       threadDelay (check * 1000000)

lastStartedKey pname name =
  T.encodeUtf8 $ pname <> "-" <> name <> "-last-started"
lockedKey pname name =
  T.encodeUtf8 $ pname <> "-" <> name <> "-running-at"

destroy :: Scheduler -> IO ()
destroy (Scheduler (Name nm) rconn mv _ _) =
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

parseUnixTime = posixSecondsToUTCTime . fromIntegral . read . T.unpack . T.decodeUtf8

renderUnixTime = T.encodeUtf8 . T.pack . show . (round :: NominalDiffTime -> Integer) . utcTimeToPOSIXSeconds

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


tryRunTask :: LockTimeout -> Text -> R.Connection -> UTCTime -> (Text, Period, IO ()) -> IO ()
tryRunTask timeout pname rconn now (name, period, task) =
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
            do task
               -- TODO(dbp 2016-05-26): Run task in
               -- thread to handle failure, log it
               -- somehow...
               R.runRedis rconn $ R.del [lockedKey pname name]
               return ()
