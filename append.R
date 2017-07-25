# setwd("D:/Dana/Others/Andy")
# getwd()
# gogoro<-read.table("NPAC_gogoro.txt", header=TRUE, sep="\t")
#----------------------------------
# 清變數資料,指定資料夾位置
#----------------------------------
rm(list=ls())
dir<-setwd("D:/Dana/4. Analysis/10. 進店查合約 part 2/data")

# detach(package:plyr) # 卸載套件
# remove.packages("plyr")

# install.packages("dplyr")   安裝後便不須執行
# install.packages("reshape2")
# library(plyr)

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

# D1<-read.csv("201702.csv")
# D2<-read.csv("201703.csv")
# D3<-read.csv("201704.csv")
# D4<-read.csv("201705.csv")
# D5<-read.csv("201706.csv")
# 
# #重新命名欄位
# colnames(D1) <- c("Subscriber_No","MINING_DW_SUBSCR_NO","Date","Time","Store_No","Store_Name","Store_Area","Staff_No")
# colnames(D2) <- c("Subscriber_No","MINING_DW_SUBSCR_NO","Date","Time","Store_No","Store_Name","Store_Area","Staff_No")
# colnames(D3) <- c("Subscriber_No","MINING_DW_SUBSCR_NO","Date","Time","Store_No","Store_Name","Store_Area","Staff_No")
# colnames(D4) <- c("Subscriber_No","MINING_DW_SUBSCR_NO","Date","Time","Store_No","Store_Name","Store_Area","Staff_No")
# colnames(D5) <- c("Subscriber_No","MINING_DW_SUBSCR_NO","Date","Time","Store_No","Store_Name","Store_Area","Staff_No")
#  
# #整併
# All<-rbind(D1,D2,D3,D4,D5)

#----------------------------------
# 新增欄位
#----------------------------------

#做F和R欄位
category <- function(x) if(nchar(x) == 7) "F" else if (nchar(x) == 4) "R" else "Error"
All$group <- sapply(All$Store_No,category)

#按客戶ID及進店時間排列??
All_sorted <- arrange(All, Subscriber_No, Date, Time)

# attach(All) 
# All_sorted <- All[order(Subscriber_No, Date, Time),] 
# detach(All) 

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

# group_by(All, Subscriber_No) %>% do(data.frame(count = count(.$Subscriber_No)))  
# All_sorted_shrink <-subset(All_merge2, count>1)

#匯入NP churn日期和合約到期月數????
# install.packages("RODBC")
library("RODBC")
ch <- odbcConnect(dsn="10.68.64.138",uid="u_daXXX",pwd="DJana8XXX")
NP_churn<-sqlQuery(ch, paste("select mining_dw_subscr_no, inactv_date, PROM_CURR_EXP_MONTH_CNT, data_month
from mds_mart.mds_active_mly
where churn_ind='Y' and churn_type='MNP' and
data_month>='2017-01-01'" ))

# NP_churn <- read.table("SQLAExport_NPchurn2017_andCT.txt", header=TRUE, sep="\t")
NP_churn$PROM_CURR_EXP_MONTH_CNT = as.numeric(NP_churn$PROM_CURR_EXP_MONTH_CNT)
All_merge3 <-merge(All_merge2,NP_churn, by = c("MINING_DW_SUBSCR_NO"), all.x = TRUE, all.y = FALSE)
# install.packages("plyr")
# library(plyr)
# All_merge3 <-join(All_merge2,NP_churn, by = c("MINING_DW_SUBSCR_NO"), 'left')
# 
# install.packages("dplyr")
# install.packages("reshape2")
# library(plyr)

# library(dplyr)
# library(reshape2)

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

# All_merge3$DateNext <- c(as.Date(All_merge3$Date[-1]), NA)
# All_merge3$MiningNext <- c(All_merge3$Subscriber_No[-1], NA)

# if(All_merge3$Subscriber_No==All_merge3$MiningNext) {
# All_merge3$DateDiff<- difftime(All_merge3$DateNext , All_merge3$Date , units="days")
#   # }  #???可以是“secs”, “mins”, “hours”, “days”
# All_merge3$DateDiff<- round(All_merge3$DateDiff)
# 
# All_merge3$ChurnDateDiff<- difftime(All_merge3$INACTV_DATE , All_merge3$Date , units="days")
# All_merge3$ChurnDateDiff<- round(All_merge3$ChurnDateDiff)


# today <- Sys.Date()
# gtd <- as.Date("2011-07-01")   
# difftime(today, gtd, units="weeks")  #???可以是“secs”, “mins”, “hours”, “days”


# data.frame(date = c(as.Date('2017-07-01'),
#                     as.Date('2017-07-02'),
#                     as.Date('2017-07-10'))) %>% 
#   tbl_df() %>% 
#   mutate(lead_date = lead(date),
#          diff_day = lead_date - date)

# 做進店順序欄位 (新增一欄位)
  All_agg <- All_merge3 %>% 
  group_by(MINING_DW_SUBSCR_NO) %>% 
  summarise(paste(group, diff_day, collapse = ''))
  
  # data.frame(date = c(as.Date('2017-07-01'),
  #                     as.Date('2017-07-02'),
  #                     as.Date('2017-07-10'))) %>%
  #   tbl_df() %>%
  #   mutate(lead_date = lead(date),
  #          leadlead_date = lead(lead_date),
  #          diff_day = lead_date - date)
  
  # mean_label_Petal.Width=ifelse(mean(Petal.Width)>0.5,"A","B")
      # paste(ChurnDateDiff, "C",collapse = ''))
    
  # flow <- function(All_merge3) 
  #   if(All_merge3$Subscriber_No==All_merge3$MiningNext) {
  #     All_agg <- All_merge3 %>% 
  #   group_by(MINING_DW_SUBSCR_NO) %>% 
  #   summarise(paste(group, DateDiff, collapse = ''))
  #     }
  # else   {
  #   All_agg <- All_merge3 %>% 
  #     group_by(MINING_DW_SUBSCR_NO) %>% 
  #     summarise(paste(group, DateDiff, collapse = ''))
  #   }
      
      
  # All_flow <- sapply(All_merge3,flow)
  # 做進店後流失前天數欄位 (新增一欄位)
  All_churnagg <- All_merge3 %>% 
    group_by(MINING_DW_SUBSCR_NO) %>% 
    summarise( paste(churn_diff_day, "C",collapse = ''))
  # paste(ChurnDateDiff, "C",collapse = ''))

  # summarise(if(All_merge3$Subscriber_No==All_merge3$MiningNext) Store_F = paste(group, DateDiff, collapse = ''))
All_merge4 <-merge(All_merge3,All_agg, by = c("MINING_DW_SUBSCR_NO"), all.x = TRUE, all.y = FALSE)
All_merge4 <-merge(All_merge4,All_churnagg, by = c("MINING_DW_SUBSCR_NO"), all.x = TRUE, all.y = FALSE)


# data <- data.frame(ID = rep('A', 2),
#                    Store = c('F', 'R'))
# data_agg <- data %>% 
#   group_by(ID) %>% 
#   summarise(Store_F = paste(Store, collapse = ''))

#有約無約欄位(churn的客戶才有此資訊)
All_merge4$PROM_CURR_EXP_MONTH_CNT<- as.numeric(All_merge4$PROM_CURR_EXP_MONTH_CNT)

All_merge4<-All_merge4 %>% 
mutate( contract_or_not = ifelse(PROM_CURR_EXP_MONTH_CNT>0,1,0) )

# write.csv(All_merge4,"All_store_Data4.csv")
# write.csv(All_sorted_shrink,"All_store_Data_shrink2.csv")
# edit(All_sorted) 
#----------------------------------
# 再做判斷進F的客戶是否三天內也進R的dataframe
#----------------------------------
All_merge4<-read.csv("All_store_Data4.csv")
All_merge4_sub = subset(All_merge4, select = c(Subscriber_No, MINING_DW_SUBSCR_NO,Date,group,Store_No,Store_Name,count))
# All_merge4_sub = subset(All_merge4, MINING_DW_SUBSCR_NO!= 0, !is.na(MINING_DW_SUBSCR_NO))
All_merge4_sub = subset(All_merge4_sub, MINING_DW_SUBSCR_NO!=0)
All_merge4_sub = subset(All_merge4_sub, MINING_DW_SUBSCR_NO!="?")
All_merge4_sub = arrange(All_merge4_sub, Subscriber_No, Date)
All_merge4_sub$Date = as.Date(All_merge4_sub$Date )


# Split Data 
mydata_F = All_merge4_sub %>%
  filter(group=='F') %>%
  select(MINING_DW_SUBSCR_NO,Store_NoF=Store_No,Store_NameF= Store_Name, StoreF=group,DateF=Date)

mydata_R = All_merge4_sub %>%
  filter(group=='R') %>%
  select(MINING_DW_SUBSCR_NO,Store_NoR=Store_No,Store_NameR= Store_Name, StoreR=group,DateR=Date)

# Find 3 Day data
mydata_3day = full_join(mydata_F,mydata_R,by="MINING_DW_SUBSCR_NO") %>%
  filter( (DateF <= DateR & DateF+3>=DateR))# | (DateR <= DateF & DateR+3>=DateF) )
write.csv(mydata_3day,"mydata_3day.csv")
# Final
mydata_both = bind_rows( mydata_3day %>% select(MINING_DW_SUBSCR_NO,Store_No=Store_NoF, Store_Name=Store_NameF,Date=DateF,group=StoreF) ,
                         mydata_3day %>% select(MINING_DW_SUBSCR_NO,Store_No=Store_NoR, Store_Name=Store_NameR,Date=DateR,group=StoreR)) %>% 
  distinct(MINING_DW_SUBSCR_NO, Store_No, Store_Name, Date,group)
mydata_both$Bad_Store = 1

write.csv(mydata_both,"mydata_both.csv")

All_merge4$Date = as.Date(All_merge4$Date )
All_merge5 <-merge(All_merge4, mydata_both, by = c("MINING_DW_SUBSCR_NO","Store_No","Store_Name","Date","group"), all.x = TRUE, all.y = FALSE)
All_merge5$Bad_Store = as.numeric(All_merge5$Bad_Store )

write.csv(All_merge5,"All_store_Data5.csv")

# 把檔案再讀進來
All_merge5<-read.csv("All_store_Data5.csv")

#Q1 有NP churn者，最後一次進店到churn的天數差 (每個churn客戶的天數))
subset_Q1<- subset(All_merge5, churn_or_not==1)
# subset_Q1<- subset(subset_Q1, Bad_Store==1)
subset_Q1<- subset(subset_Q1, last_one==1)
subset_Q1<- subset(subset_Q1, churn_diff_day>0)
subset_Q1<- subset(subset_Q1, count<=30)

subset_Q1 <- subset_Q1 %>% 
  group_by(MINING_DW_SUBSCR_NO,churn_diff_day) %>% 
  summarise( count())

write.csv(subset_Q1,"subset_Q1.csv")

#Q2_1 有NP churn者，只進過F且churn時無約的客戶 (每家F的churn客戶數)?)
subset_Q2_1<- subset(All_merge5, churn_or_not==1)
subset_Q2_1<- subset(subset_Q2_1, contract_or_not==0)
subset_Q2_1<- subset(subset_Q2_1,F>0)
subset_Q2_1<- subset(subset_Q2_1,R==0)
subset_Q2_1<- subset(subset_Q2_1,churn_diff_day>0)
subset_Q2_1<- subset(subset_Q2_1,churn_diff_day<=7)
subset_Q2_1<- subset(subset_Q2_1, count<=30)

# Q2_1分子
subset_Q2_1_groupby <- subset_Q2_1 %>% 
  group_by(Store_No,Store_Name,MINING_DW_SUBSCR_NO) %>% 
  summarise( count = n())

subset_Q2_1_groupbygroupby <- subset_Q2_1_groupby %>% 
  group_by(Store_No,Store_Name) %>% 
  summarise( 分子_count = n())

write.csv(subset_Q2_1_groupbygroupby,"subset_Q2_1.csv")

#Q2_2 有NP churn者，有進過F和R且churn時還有約的客戶 (每家F的churn客戶數)?)
subset_Q2_2<- subset(All_merge5, churn_or_not==1)
subset_Q2_2<- subset(subset_Q2_2, contract_or_not==1)
subset_Q2_2<- subset(subset_Q2_2,F_and_R>0)
subset_Q2_2<- subset(subset_Q2_2,churn_diff_day>0)
subset_Q2_2<- subset(subset_Q2_2,churn_diff_day<=7)
subset_Q2_2<- subset(subset_Q2_2,Bad_Store==1)
subset_Q2_2<- subset(subset_Q2_2, count<=30)
# Q2_2分子
subset_Q2_2_groupby <- subset_Q2_2 %>% 
  group_by(Store_No,Store_Name,MINING_DW_SUBSCR_NO) %>% 
  summarise( count = n())

subset_Q2_2_groupbygroupby <- subset_Q2_2_groupby %>% 
  group_by(Store_No,Store_Name) %>% 
  summarise( 分子_count = n())

write.csv(subset_Q2_2_groupbygroupby,"subset_Q2_2.csv")

# 分母 (每家F的進店客戶數)
subset_Q2_groupby <- All_merge4 %>% 
  group_by(Store_No,Store_Name,MINING_DW_SUBSCR_NO) %>% 
  summarise( count = n())

subset_Q2_groupbygroupby <- subset_Q2_groupby %>% 
  group_by(Store_No,Store_Name) %>% 
  summarise( 分母_count = n())

write.csv(subset_Q2_groupbygroupby,"subset_Q2.csv")



# 進店人分群 (1.三天內進F&R且有約，2.只進F無約，3.其他進店客戶))




# FR_in_3days = function (data, count)
#     for (a in seq(1,count)){
#       All_merge3<-All_merge3 %>% 
#         mutate(lead_one = lead(Subscriber_No),
#                last_one = ifelse(Subscriber_No== lead_one,0,1))
#     }
#    }
  
# noquote(strsplit("A text I want to display with spaces", NULL)[[1]])
# 
# x <- c(as = "asfef", qu = "qwerty", "yuiop[", "b", "stuff.blah.yech")
# # split x on the letter e
# strsplit(x, "e")
