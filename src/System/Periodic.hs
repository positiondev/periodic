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
run (Scheduler nm rconn mv) = forever $ do tasks <- readMVar mv
                                           mapM_ (tryRunTask nm rconn) tasks
                                           threadDelay checkPeriod

needsRun :: Period -> UTCTime -> Maybe UTCTime -> Maybe UTCTime -> Bool
needsRun (Daily (Time t)) now Nothing Nothing
  | abs (utctDayTime now - t) < fromIntegral checkPeriod * 10
  = True
needsRun (Daily (Time t)) now (Just last) Nothing
  | abs (utctDayTime now - t) < fromIntegral checkPeriod * 10 &&
    abs (diffUTCTime now last) > fromIntegral lockTimeout
  = True
needsRun (Daily (Time t)) now _ (Just locked)
  | abs (diffUTCTime now locked) > fromIntegral lockTimeout
  = True
needsRun _ _ _ _
  = False

nextRunTime :: Period -> UTCTime -> Maybe UTCTime -> UTCTime
nextRunTime (Daily (Time t)) now Nothing = now { utctDayTime = t }
nextRunTime (Daily (Time t)) now (Just last) =
  let next = last {utctDay = addDays 1 (utctDay last)}
      adjusted = next { utctDayTime = t}
  -- NOTE(dbp 2016-05-22):
  in if abs (diffUTCTime next adjusted) < 1 * fromIntegral hours
        then adjusted
        else next

collapseError :: Either a (Maybe b) -> Maybe b
collapseError (Left _) = Nothing
collapseError (Right v) = v

parseUnixTime = posixSecondsToUTCTime . fromIntegral . read . T.unpack . T.decodeUtf8


tryRunTask :: Text -> R.Connection -> (Text, Period, IO ()) -> IO ()
tryRunTask pname rconn (name, period, task) =
  do let key = T.encodeUtf8 $ pname <> "-" <> name <> "-last-run"
     let locked = T.encodeUtf8 $ pname <> "-" <> name <> "-running-at"
     now <- getCurrentTime
     lastrun <- fmap parseUnixTime . collapseError <$> R.runRedis rconn (R.get key)
     let nextRun = nextRunTime period now lastrun
     -- NOTE(dbp 2016-05-22):
     -- 1. compute when it should next run
     -- 2. atomically lock it if need to run and not locked.
     (k,l) <- R.runRedis rconn $ do kr <- R.get key
                                    lr <- R.get locked
                                    case (kr,lr) of
                                      (Left _, Left _) -> return (Nothing, Nothing)
                                      (Right r, Left _) -> return (r, Nothing)
                                      (Left _, Right r) -> return (Nothing, r)
                                      (Right r1, Right r2) -> return (r1, r2)
     now <- getCurrentTime
     when (needsRun period now
                           ((posixSecondsToUTCTime . fromIntegral . read . T.unpack . T.decodeUtf8) <$> k)
                           ((posixSecondsToUTCTime . fromIntegral . read . T.unpack . T.decodeUtf8) <$> l)) $
       do let tstr = T.encodeUtf8 $ T.pack $ show $ utcTimeToPOSIXSeconds now
          R.runRedis rconn $ R.set locked tstr
          task
          R.runRedis rconn $ do R.set key tstr
                                R.del [locked]
          return ()
