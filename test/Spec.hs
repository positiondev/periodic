{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
import           Control.Concurrent      (forkIO, killThread, threadDelay)
import           Control.Concurrent.MVar (MVar, modifyMVarMasked_, newMVar,
                                          readMVar, takeMVar)
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

main :: IO ()
main = hspec $
  do describe "shouldRun" $
       do it "should run if it's not locked and next time is near now" $
            shouldRun 100 Nothing (date 2016 5 5) (time (date 2016 5 5) 0 0 10)
          it "shouldn't run if it was locked recently" $
            not $ shouldRun 100 (Just (date 2016 5 5))
                                (time (date 2016 5 5) 0 3 0)
                                (time (date 2016 5 5) 0 0 10)
          it "should run if it was locked a long time ago" $
            shouldRun 100 (Just (date 2016 5 4))
                          (date 2016 5 5)
                          (time (date 2016 5 5) 0 0 10)
     describe "simple job" $
       do it "should run " $
            do mvar <- newMVar 0
               rconn <- R.connect R.defaultConnectInfo
               scheduler <- create "simple-1" rconn 1
               addTask scheduler "job-1" (Every (Seconds 100)) (modifyMVarMasked_ mvar (return . (+1)))

               wthread <- forkIO (run scheduler)
               threadDelay 30000
               killThread wthread
               destroy scheduler
               v <- takeMVar mvar
               1 `shouldBe` v
          it "should only run once per scheduled time" $
             do mvar <- newMVar 0
                rconn <- R.connect R.defaultConnectInfo
                scheduler <- create "simple-2" rconn 1
                addTask scheduler "job-2" (Every (Seconds 100)) (modifyMVarMasked_ mvar (return . (+1)))
                wthread1 <- forkIO (run scheduler)
                wthread2 <- forkIO (run scheduler)
                wthread3 <- forkIO (run scheduler)
                threadDelay 100000
                killThread wthread1
                killThread wthread2
                killThread wthread3
                destroy scheduler
                v <- takeMVar mvar
                v `shouldBe` 1
          it "should run at each time point" $
             do mvar <- newMVar 0
                rconn <- R.connect R.defaultConnectInfo
                scheduler <- create "simple-3" rconn 1
                addTask scheduler "job-3" (Every (Seconds 3)) (modifyMVarMasked_ mvar (return . (+1)))
                wthread1 <- forkIO (run scheduler)
                wthread2 <- forkIO (run scheduler)
                wthread3 <- forkIO (run scheduler)
                threadDelay 10000000
                killThread wthread1
                killThread wthread2
                killThread wthread3
                destroy scheduler
                v <- takeMVar mvar
                v `shouldBe` 3
     --      it "queueing 2 jobs should increment twice" $
     --        do mvar <- newMVar 0
     --           hworker <- createWith (conf "simpleworker-2"
     --                                       (SimpleState mvar))
     --           wthread <- forkIO (worker hworker)
     --           queue hworker SimpleJob
     --           queue hworker SimpleJob
     --           threadDelay 40000
     --           killThread wthread
     --           destroy hworker
     --           v <- takeMVar mvar
     --           assertEqual "State should be 2 after 2 jobs run" 2 v
     --      it "queueing 1000 jobs should increment 1000" $
     --        do mvar <- newMVar 0
     --           hworker <- createWith (conf "simpleworker-3"
     --                                       (SimpleState mvar))
     --           wthread <- forkIO (worker hworker)
     --           replicateM_ 1000 (queue hworker SimpleJob)
     --           threadDelay 2000000
     --           killThread wthread
     --           destroy hworker
     --           v <- takeMVar mvar
     --           assertEqual "State should be 1000 after 1000 job runs" 1000 v
     --      it "should work with multiple workers" $
     --      -- NOTE(dbp 2015-07-12): This probably won't run faster, because
     --      -- they are all blocking on the MVar, but that's not the point.
     --        do mvar <- newMVar 0
     --           hworker <- createWith (conf "simpleworker-4"
     --                                       (SimpleState mvar))
     --           wthread1 <- forkIO (worker hworker)
     --           wthread2 <- forkIO (worker hworker)
     --           wthread3 <- forkIO (worker hworker)
     --           wthread4 <- forkIO (worker hworker)
     --           replicateM_ 1000 (queue hworker SimpleJob)
     --           threadDelay 1000000
     --           killThread wthread1
     --           killThread wthread2
     --           killThread wthread3
     --           killThread wthread4
     --           destroy hworker
     --           v <- takeMVar mvar
     --           assertEqual "State should be 1000 after 1000 job runs" 1000 v
