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
                scheduler <- create (Name "simple-2") rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000))
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
                scheduler <- create (Name "simple-3") rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000))
                addTask scheduler "job-3" (Every (Seconds 1)) (modifyMVarMasked_ mvar (return . (+1)))
                wthread1 <- forkIO (run scheduler)
                wthread2 <- forkIO (run scheduler)
                wthread3 <- forkIO (run scheduler)
                threadDelay 2900000
                killThread wthread1
                killThread wthread2
                killThread wthread3
                destroy scheduler
                v <- takeMVar mvar
                v `shouldBe` 3
