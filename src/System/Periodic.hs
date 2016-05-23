{-# LANGUAGE OverloadedStrings #-}

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

data Scheduler = Scheduler Text R.Connection (MVar [(Text, Period, IO ())])

newtype Time = Time DiffTime -- time from midnight UTC

data Period = Daily Time

create :: Text -> R.Connection -> IO Scheduler
create name rconn = do mv <- newMVar []
                       return $ Scheduler name rconn mv

schedule :: Text -> Period -> Scheduler -> IO () -> IO ()
schedule job when (Scheduler _ _ mv) act =
  do modifyMVar_ mv (return . (:) (job, when, act))
     return ()

seconds = 1000000
day = 86400 * seconds
hours = 3600 * seconds
checkPeriod = 100 * seconds
lockTimeout = 10 * hours

run :: Scheduler -> IO ()
run (Scheduler nm rconn mv) = forever $ do now <- getCurrentTime
                                           tasks <- readMVar mv
                                           mapM_ (tryRunTask nm rconn now) tasks
                                           threadDelay checkPeriod

nextRunTime :: Period -> UTCTime -> UTCTime
nextRunTime (Daily (Time t)) now = now { utctDayTime = t }

collapseError :: Either a (Maybe b) -> Maybe b
collapseError (Left _) = Nothing
collapseError (Right v) = v

collapseNumberBoolFalse :: Either a (Maybe Integer) -> Bool
collapseNumberBoolFalse (Left _) = False
collapseNumberBoolFalse (Right (Just 1)) = True
collapseNumberBoolFalse (Right (Just 0)) = False

parseUnixTime = posixSecondsToUTCTime . fromIntegral . read . T.unpack . T.decodeUtf8

renderUnixTime = T.encodeUtf8 . T.pack . show . utcTimeToPOSIXSeconds

tryRunTask :: Text -> R.Connection -> UTCTime -> (Text, Period, IO ()) -> IO ()
tryRunTask pname rconn now (name, period, task) =
  do let key = T.encodeUtf8 $ pname <> "-" <> name <> "-last-run"
     let locked = T.encodeUtf8 $ pname <> "-" <> name <> "-running-at"
     let nextRun = nextRunTime period now
     shouldRun <- collapseNumberBoolFalse <$> R.runRedis rconn
       -- NOTE(dbp 2016-05-22): This is the heart of this whole
       -- library. We check if should run, which involves checking the
       -- last time that it has run (and if it is close to when the
       -- next scheduled run is) and simultaneously if it has been
       -- locked. Note that since we need to handle the fact that a
       -- worker could have claimed it and then died, we have to check
       -- even when we are not anywhere near the actual scheduled
       -- time.
       (R.eval "local last = redis.call('get', KEYS[1])\n\
               \local lock = redis.call('get', KEYS[2])\n\
               \if lock ~= nil and math.abs(ARGV[1] - lock) > ARGV[4] then\n\
               \  redis.call('set', KEYS[1], ARGV[1])\n\
               \  redis.call('set', KEYS[2], ARGV[1])\n\
               \  return 1\n\
               \elseif math.abs(ARGV[1] - ARGV[2]) < ARGV[3] and (last == nil or math.abs(last - ARGV[1]) > ARGV[4]) and lock == nil then\n\
               \  redis.call('set', KEYS[1], ARGV[1])\n\
               \  redis.call('set', KEYS[2], ARGV[1])\n\
               \  redis.call(''\n\
               \  return 1\n\
               \else\n\
               \  return 0\
               \end" [key, locked] [renderUnixTime now, renderUnixTime nextRun, T.encodeUtf8 . T.pack . show $ 10 * checkPeriod, T.encodeUtf8 . T.pack . show $ lockTimeout])
     when shouldRun $ do task
                         R.runRedis rconn $ R.del [locked]
                         return ()
