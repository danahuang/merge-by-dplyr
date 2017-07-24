#----------------------------------
# 清變數資料,指定資料夾位置
#----------------------------------
rm(list=ls())
dir<-setwd("D:/Dana/4. Analysis/10. 進店查合約 part 2/data")

library(data.table)
library(dplyr)
library(reshape2)


#----------------------------------
# 匯入資料
#----------------------------------

#匯入每月進店資料

dir_files<-dir(path = dir)
All=data.frame()
for( i in (1:length(dir_files))){
  if (regexpr("2017",dir_files[i])[1]==1){
    D<-read.csv(dir_files[i])
    colnames(D) <- c("Subscriber_No","MINING_DW_SUBSCR_NO","Date","Time","Store_No","Store_Name","Store_Area","Staff_No")
    All<-rbind(All,D) }
  else{
    next
  }
}

#----------------------------------
# 新增欄位
#----------------------------------


#做F和R欄位
category <- function(x) if(nchar(x) == 7) "F" else if (nchar(x) == 4) "R" else "Error"
All$group <- sapply(All$Store_No,category)

#按客戶ID及進店時間排列??
All_sorted <- arrange(All, Subscriber_No, Date, Time)

# 計算客戶進店次數
library(dplyr)
count <- All_sorted  %>% group_by(Subscriber_No) %>% summarise(count = n())
All_merge <-merge(All_sorted,count, by = c("Subscriber_No"), all.x = TRUE, all.y = FALSE)

# 是否有進F或R (新增二欄位))
FR <-dcast(All_merge,formula=Subscriber_No~group, value.var="count")
All_merge2 <-merge(All_merge,FR, by = c("Subscriber_No"), all.x = TRUE, all.y = FALSE)

# 是否有進F且R (新增一欄位)
All_merge2 <-
  All_merge2 %>% 
  mutate(F_and_R = All_merge2$F*All_merge2$R )

#匯入NP churn日期和合約到期月數????
# install.packages("RODBC")
library("RODBC")
ch <- odbcConnect(dsn="10.68.64.138",uid="u_danahuang1",pwd="DJana8501421")
NP_churn<-sqlQuery(ch, paste("select mining_dw_subscr_no, inactv_date, PROM_CURR_EXP_MONTH_CNT, data_month
                             from mds_mart.mds_active_mly
                             where churn_ind='Y' and churn_type='MNP' and
                             data_month>='2017-01-01'" ))

# NP_churn <- read.table("SQLAExport_NPchurn2017_andCT.txt", header=TRUE, sep="\t")
NP_churn$PROM_CURR_EXP_MONTH_CNT = as.numeric(NP_churn$PROM_CURR_EXP_MONTH_CNT)
All_merge3 <-merge(All_merge2,NP_churn, by = c("MINING_DW_SUBSCR_NO"), all.x = TRUE, all.y = FALSE)

#和下一筆的天數差 (新增一欄位)
All_merge3<-All_merge3 %>% 
  tbl_df() %>% 
  mutate(lead_date = lead(Date),
         diff_day = as.Date(lead_date) - as.Date(Date))

#該筆進店日和churn日期的天數差 (新增一欄位)
All_merge3$churn_diff_day<-as.Date(All_merge3$INACTV_DATE) - as.Date(All_merge3$Date)

#是否有churn (新增一欄位)
All_merge3 <-
  All_merge3 %>% 
  mutate(churn_or_not = ifelse(is.na(INACTV_DATE),0,1) )

#是否為該客戶最後一筆 (新增一欄位)
All_merge3<-All_merge3 %>% 
  mutate(lead_one = lead(Subscriber_No),
         last_one = ifelse(Subscriber_No== lead_one,0,1))

All_merge5<-All_merge3

# All_merge5<-read.csv("All_store_Data5.csv")
All_merge5_sub<- subset (All_merge5, as.Date(Date)>='2017-05-01' )
All_merge5_sub<- subset (All_merge5_sub, as.Date(Date)<='2017-05-31' )


All_merge5_sub<- subset(All_merge5_sub, MINING_DW_SUBSCR_NO!=0)
All_merge5_sub<- subset(All_merge5_sub, MINING_DW_SUBSCR_NO!="?")
All_merge5_sub <-
  All_merge5_sub %>% 
  mutate(churn_or_not_in_15days = ifelse(churn_diff_day>0 & churn_diff_day<=15 ,1,0) )

# Test<-subset(All_merge5_sub,All_merge5_sub$churn_or_not_in_15days==1)
# Test<-subset(Test,Test$last_one==0)


All_merge5_sub_15days<- subset(All_merge5_sub, churn_or_not_in_15days==1)
# All_merge5_sub<- subset(All_merge5_sub, last_one==1)
All_merge5_sub_Store_Name <- All_merge5_sub_15days %>% 
  group_by(Store_Name,Store_No) %>% 
  summarise(count = n_distinct(MINING_DW_SUBSCR_NO))

All_merge5_sub_Store_Name_M <- All_merge5_sub %>% 
  group_by(Store_Name,Store_No) %>% 
  summarise(count_M = n_distinct(MINING_DW_SUBSCR_NO))

# All_merge5_sub_Store_Name_???1 <- All_merge5_sub_Store_Name_???? %>% 
#   group_by(Store_Name) %>% 
#   summarise(count_M = n())

All_merge <-merge(All_merge5_sub_Store_Name,All_merge5_sub_Store_Name_M, by = c("Store_Name","Store_No"), all.x = TRUE, all.y = FALSE)
write.csv(All_merge,"15days_F_List_Y17May.csv")
