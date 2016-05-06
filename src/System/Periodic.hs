{-# LANGUAGE OverloadedStrings #-}

module System.Periodic where

import           Control.Concurrent      (threadDelay)
import           Control.Concurrent.MVar
import           Control.Monad           (forever, join, when)
import           Data.Monoid
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import qualified Database.Redis          as R

data Scheduler = Scheduler Text R.Connection (MVar [(Text, Period, IO ())])

newtype Time = Time DiffTime -- time from midnight UTC
newtype Day = Day Int -- day from befinning of month
newtype WeekDay = WeekDay Int -- 0 is Sunday, 6 is Saturday

data Period = Daily Time | Weekly WeekDay Time | Monthly Day Time

create :: Text -> IO Scheduler
create = undefined

schedule :: Text -> Period -> Scheduler -> IO () -> IO ()
schedule = undefined

run :: Scheduler -> IO ()
run (Scheduler nm rconn mv) = forever $ do tasks <- readMVar mv
                                           mapM_ (tryRunTask nm rconn) tasks
                                           threadDelay 10000000 -- ten seconds

needsRun :: Period -> Maybe UTCTime -> Maybe UTCTime -> Bool
needsRun _ _ _ = False

tryRunTask :: Text -> R.Connection -> (Text, Period, IO ()) -> IO ()
tryRunTask pname rconn (name, period, task) =
  do let key = T.encodeUtf8 $ pname <> "-" <> name <> "-last-run"
     let locked = T.encodeUtf8 $ pname <> "-" <> name <> "-running-at"
     (k,l) <- R.runRedis rconn $ do kr <- R.get key
                                    lr <- R.get locked
                                    case (kr,lr) of
                                      (Left _, Left _) -> return (Nothing, Nothing)
                                      (Right r, Left _) -> return (r, Nothing)
                                      (Left _, Right r) -> return (Nothing, r)
                                      (Right r1, Right r2) -> return (r1, r2)
     when (needsRun period ((posixSecondsToUTCTime . fromIntegral . read . T.unpack . T.decodeUtf8) <$> k)
                           ((posixSecondsToUTCTime . fromIntegral . read . T.unpack . T.decodeUtf8) <$> l)) $
       do now <- getCurrentTime
          let tstr = T.encodeUtf8 $ T.pack $ show $ utcTimeToPOSIXSeconds now
          R.runRedis rconn $ R.set locked tstr
          task
          R.runRedis rconn $ do R.set key tstr
                                R.del [locked]
          return ()
