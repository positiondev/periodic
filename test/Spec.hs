{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
import           Control.Concurrent      (forkIO, killThread, threadDelay)
import           Control.Concurrent.MVar (MVar, modifyMVarMasked_, newMVar,
                                          readMVar, takeMVar)
import           Control.Monad           (replicateM)
import qualified Data.Text               as T
import           Data.Time.Calendar
import           Data.Time.Clock
import qualified Database.Redis          as R
import           System.Periodic

import           Test.Hspec


date :: Integer -> Int -> Int -> UTCTime
date y m d = UTCTime (fromGregorian y m d) 0

time :: UTCTime -> Int -> Int -> Int -> UTCTime
time t h m s = t { utctDayTime = fromIntegral $ h * 60 * 60 + m * 60 + s }

runNThreads :: Int -> Int -> Scheduler -> IO ()
runNThreads n delay sched = do threads <- replicateM n (forkIO $ run sched)
                               threadDelay delay
                               mapM_ killThread threads
main :: IO ()
main = hspec $
  do describe "shouldRun" $
       do it "should run if it's never been run, not locked and time just passed" $
            shouldRun (LockTimeout (Seconds 1000)) (Daily (Time 5)) Nothing Nothing (time (date 2016 5 5) 0 0 6)
          it "shouldn't run if it was locked recently" $
            not $ shouldRun (LockTimeout (Seconds 1000)) (Daily (Time 5)) (Just (time (date 2016 5 4) 23 45 0)) (Just (time (date 2016 5 4) 23 45 0)) (time (date 2016 5 5) 0 0 6)
          it "should run if it was locked a long time ago" $
            shouldRun (LockTimeout (Seconds 1000))
                      (Daily (Time 5))
                      (Just (date 2016 5 4))
                      (Just (date 2016 5 4))
                      (date 2016 5 5)
     describe "simple job" $
       do it "should run " $
            do mvar <- newMVar 0
               rconn <- R.connect R.defaultConnectInfo
               scheduler <- create (Name "simple-1") rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000))
               addTask scheduler "job" (Every (Seconds 100)) (modifyMVarMasked_ mvar (return . (+1)))

               runNThreads 1 30000 scheduler

               destroy scheduler
               v <- takeMVar mvar
               1 `shouldBe` v
          it "should only run once per scheduled time" $
             do mvar <- newMVar 0
                rconn <- R.connect R.defaultConnectInfo
                scheduler <- create (Name "simple-2") rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000))
                addTask scheduler "job" (Every (Seconds 100)) (modifyMVarMasked_ mvar (return . (+1)))
                runNThreads 3 100000 scheduler
                destroy scheduler
                v <- takeMVar mvar
                v `shouldBe` 1
          it "should run at each time point" $
             do mvar <- newMVar 0
                rconn <- R.connect R.defaultConnectInfo
                scheduler <- create (Name "simple-3") rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000))
                addTask scheduler "job" (Every (Seconds 2)) (modifyMVarMasked_ mvar (return . (+1)))
                runNThreads 3 4000000 scheduler
                destroy scheduler
                v <- takeMVar mvar
                -- NOTE(dbp 2016-05-26): Precise timing is hard
                -- without making the tests super slow.
                (v == 3 || v == 2) `shouldBe` True
          it "should run at scheduled time" $
             do mvar <- newMVar 0
                rconn <- R.connect R.defaultConnectInfo
                scheduler <- create (Name "simple-4") rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000))
                seconds <- utctDayTime <$> getCurrentTime
                addTask scheduler "job" (Daily (Time seconds)) (modifyMVarMasked_ mvar (return . (+1)))
                runNThreads 1 4000000 scheduler
                destroy scheduler
                v <- takeMVar mvar
                v `shouldBe` 1
          it "should not run at an unscheduled time" $
             do mvar <- newMVar 0
                rconn <- R.connect R.defaultConnectInfo
                scheduler <- create (Name "simple-4") rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000))
                now <- getCurrentTime
                let seconds = utctDayTime $ addUTCTime 3600 now
                addTask scheduler "job" (Daily (Time seconds)) (modifyMVarMasked_ mvar (return . (+1)))
                runNThreads 1 2000000 scheduler
                destroy scheduler
                v <- takeMVar mvar
                v `shouldBe` 0
     describe "error handling" $
       do it "should keep running jobs if one throws an exception" $
            do mvar <- newMVar 0
               rconn <- R.connect R.defaultConnectInfo
               scheduler <- create (Name "error-1") rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000))
               addTask scheduler "job-error" (Every (Seconds 100)) (error "blowing up")
               thread <- forkIO $ run scheduler
               threadDelay 300000
               addTask scheduler "job-success" (Every (Seconds 100)) (modifyMVarMasked_ mvar (return . (+1)))
               threadDelay 2000000
               killThread thread
               destroy scheduler
               v <- takeMVar mvar
               1 `shouldBe` v
