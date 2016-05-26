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

data Scheduler = Scheduler Text R.Connection (MVar [(Text, Period, IO ())]) Int

newtype Time = Time DiffTime -- time from midnight UTC

newtype Seconds = Seconds Int

data Period = Daily Time | Every Seconds

create :: Text -> R.Connection -> Int -> IO Scheduler
create name rconn check = do mv <- newMVar []
                             return $ Scheduler name rconn mv check

addTask :: Scheduler -> Text -> Period -> IO () -> IO ()
addTask (Scheduler _ _ mv _) job when act =
  do modifyMVar_ mv (return . (:) (job, when, act))
     return ()

lockTimeout = 10 * 3600

run :: Scheduler -> IO ()
run (Scheduler nm rconn mv check) = forever $
    do now <- getCurrentTime
       tasks <- readMVar mv
       mapM_ (tryRunTask check nm rconn now) tasks
       threadDelay (check * 1000000)

lastRunKey pname name =
  T.encodeUtf8 $ pname <> "-" <> name <> "-last-run"
lockedKey pname name =
  T.encodeUtf8 $ pname <> "-" <> name <> "-running-at"

destroy :: Scheduler -> IO ()
destroy (Scheduler nm rconn mv _) =
  do tasks <- readMVar mv
     R.runRedis rconn $
       R.del (concatMap (\(tnm,_,_) ->
                            [lastRunKey nm tnm
                            ,lockedKey nm tnm])
                        tasks)
     return ()

nextRunTime :: Period -> UTCTime -> Maybe UTCTime -> UTCTime
nextRunTime (Daily (Time t)) now _ = now { utctDayTime = t }
nextRunTime (Every (Seconds n)) now Nothing = now
nextRunTime (Every (Seconds n)) now (Just last) =
  now { utctDayTime = utctDayTime last + fromIntegral n}

collapseError :: Either a (Maybe b) -> Maybe b
collapseError (Left _) = Nothing
collapseError (Right v) = v

collapseNumberBoolFalse :: Either R.Reply (Maybe Integer) -> Bool
collapseNumberBoolFalse (Left e) = error (show e)
collapseNumberBoolFalse (Right (Just 1)) = True
collapseNumberBoolFalse (Right (Just 0)) = False

parseUnixTime = posixSecondsToUTCTime . fromIntegral . read . T.unpack . T.decodeUtf8

renderUnixTime = T.encodeUtf8 . T.pack . show . (round :: NominalDiffTime -> Integer) . utcTimeToPOSIXSeconds

close :: NominalDiffTime -> UTCTime -> UTCTime -> Bool
close interval a b = abs (diffUTCTime a b) < interval

further :: NominalDiffTime -> UTCTime -> UTCTime -> Bool
further interval a b = not (close interval a b)

shouldRun :: Period -> Maybe UTCTime -> Maybe UTCTime -> UTCTime -> Bool
shouldRun (Daily (Time t)) Nothing locked now
  | now > (now { utctDayTime = t }) && diffUTCTime now (now { utctDayTime = t }) < 60
  = True
shouldRun (Daily (Time t)) (Just last) locked now
  | now > (now { utctDayTime = t })
  = True
shouldRun period last locked no = False

tryRunTask :: Int -> Text -> R.Connection -> UTCTime -> (Text, Period, IO ()) -> IO ()
tryRunTask check pname rconn now (name, period, task) =
  do lastRun <- fmap parseUnixTime . collapseError <$>
                R.runRedis rconn
                           (R.get (lastRunKey pname name))
     lockedAt <- fmap parseUnixTime . collapseError <$>
                 R.runRedis rconn
                            (R.get (lockedKey pname name))
     let nextRun = nextRunTime period now lastRun
     print (lastRun, now, nextRun, shouldRun period lastRun lockedAt now)
     when (shouldRun period lastRun lockedAt now) $
       do gotLock <-
            collapseNumberBoolFalse <$> R.runRedis rconn
            (R.eval "local lock = redis.call('get', KEYS[1])\n\
                    \if (lock == false and ARGV[1] == '0') or (lock == ARGV[1]) then\n\
                    \  redis.call('set', KEYS[1], ARGV[2])\n\
                    \  return 1\n\
                    \else\n\
                    \  return 0\n\
                    \end" [lockedKey pname name]
                          [maybe "0" renderUnixTime lockedAt
                          , renderUnixTime now])
          print gotLock
          when gotLock $
            do task
               -- TODO(dbp 2016-05-26): Run task in
               -- thread to handle failure, log it
               -- somehow...
               r <- R.runRedis rconn
                               (R.eval "redis.call('del', KEYS[1])\n\
                                       \redis.call('set', KEYS[2], ARGV[1])"
                                       [ lockedKey pname name
                                       , lastRunKey pname name]
                                       [renderUnixTime now])
                 :: IO (Either R.Reply (Maybe Integer))
               print r
               return ()
