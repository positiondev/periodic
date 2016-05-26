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

newtype Seconds = Seconds Int

data Period = Daily Time | Every Seconds

create :: Text -> R.Connection -> IO Scheduler
create name rconn = do mv <- newMVar []
                       return $ Scheduler name rconn mv

schedule :: Text -> Period -> Scheduler -> IO () -> IO ()
schedule job when (Scheduler _ _ mv) act =
  do modifyMVar_ mv (return . (:) (job, when, act))
     return ()

checkPeriod = 100
lockTimeout = 10 * 3600

run :: Scheduler -> IO ()
run (Scheduler nm rconn mv) = forever $ do now <- getCurrentTime
                                           tasks <- readMVar mv
                                           mapM_ (tryRunTask nm rconn now) tasks
                                           threadDelay (checkPeriod * 1000000)

lastRunKey pname name = T.encodeUtf8 $ pname <> "-" <> name <> "-last-run"
lockedAtKey pname name = T.encodeUtf8 $ pname <> "-" <> name <> "-running-at"

destroy :: Scheduler -> IO ()
destroy (Scheduler nm rconn mv) = do tasks <- readMVar mv
                                     R.runRedis rconn $ R.del (concatMap (\(tnm,_,_) -> [lastRunKey nm tnm, lockedAtKey nm tnm]) tasks)
                                     return ()

nextRunTime :: Period -> UTCTime -> Maybe UTCTime -> UTCTime
nextRunTime (Daily (Time t)) now _ = now { utctDayTime = t }
nextRunTime (Every (Seconds n)) now Nothing = now
nextRunTime (Every (Seconds n)) now (Just last) =
  now { utctDayTime = utctDayTime last + fromIntegral n}

collapseError :: Either a (Maybe b) -> Maybe b
collapseError (Left _) = Nothing
collapseError (Right v) = v

collapseNumberBoolFalse :: Either a (Maybe Integer) -> Bool
collapseNumberBoolFalse (Left _) = False
collapseNumberBoolFalse (Right (Just 1)) = True
collapseNumberBoolFalse (Right (Just 0)) = False

parseUnixTime = posixSecondsToUTCTime . fromIntegral . read . T.unpack . T.decodeUtf8

renderUnixTime = T.encodeUtf8 . T.pack . show . utcTimeToPOSIXSeconds

close :: NominalDiffTime -> UTCTime -> UTCTime -> Bool
close interval a b = abs (diffUTCTime a b) < interval

further :: NominalDiffTime -> UTCTime -> UTCTime -> Bool
further interval a b = not (close interval a b)

shouldRun :: Maybe UTCTime -> UTCTime -> UTCTime -> Bool
shouldRun Nothing now next
  | close (fromIntegral checkPeriod * 5) now next
  = True
shouldRun (Just locked) now next
  | further (fromIntegral lockTimeout) now locked &&
    close (fromIntegral checkPeriod * 5) now next
  = True
shouldRun _ _ _ = False

tryRunTask :: Text -> R.Connection -> UTCTime -> (Text, Period, IO ()) -> IO ()
tryRunTask pname rconn now (name, period, task) =
  do let lastRunKey = T.encodeUtf8 $ pname <> "-" <> name <> "-last-run"
     let lockedAtKey = T.encodeUtf8 $ pname <> "-" <> name <> "-running-at"
     lastRun <- fmap parseUnixTime . collapseError <$>
                R.runRedis rconn (R.get lastRunKey)
     lockedAt <- fmap parseUnixTime . collapseError <$>
                 R.runRedis rconn (R.get lockedAtKey)
     let nextRun = nextRunTime period now lastRun
     when (shouldRun lockedAt now nextRun) $
       do gotLock <-
            collapseNumberBoolFalse <$> R.runRedis rconn
            (R.eval "local lock = redis.call('get', KEYS[1])\n\
                    \if (lock == nil && ARGV[1] == 0) || (lock == ARGV[1]) then\n\
                    \  redis.call('set', KEYS[1], ARGV[2])\n\
                    \  return 1\n\
                    \else\n\
                    \  return 0\n\
                    \end" [lockedAtKey]
                          [maybe "0" renderUnixTime lockedAt
                          , renderUnixTime now])
          when gotLock $ do task
                            -- TODO(dbp 2016-05-26): Run task in
                            -- thread to handle failure, log it
                            -- somehow...
                            R.runRedis rconn (R.eval "redis.call('del', KEYS[1])\n\
                                                     \redis.call('set', KEYS[2], ARGV[1])"
                                                     [lockedAtKey, lastRunKey]
                                                     [renderUnixTime now])
                              :: IO (Either R.Reply (Maybe Integer))
                            return ()
