
#----------------------------------
# 睲跑计戈,﹚戈Ж竚
#----------------------------------
rm(list=ls())
dir<-setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data")

# install.packages("data.table")
library(data.table)
library(dplyr)
library(reshape2)


# Q1 1/18~4/30 Τ秈┍NP churn 秈程產┍埌NP churnぱ计暩
All_merge5<-read.csv("All_store_Data5.csv")
# All_merge5<-fread("All_store_Data5.txt",sep='\t',header=TRUE)#,stringsAsFactors=FALSE)#,select=c(1,4,5,6,7),colClasses=c("as.numeric","as.character","as.numeric","as.Date","as.Factor")
# All_merge5_<-read.table("All_store_Data5.txt",sep='\t',header=TRUE)
All_merge5_sub<- subset (All_merge5, as.Date(Date)>='2017-01-01' )
All_merge5_sub<- subset (All_merge5_sub, as.Date(Date)<='2017-04-30' )
All_merge5_sub<- subset (All_merge5_sub, last_one==1 )
Q1<- subset (All_merge5_sub, as.numeric(churn_diff_day)>=0 )
Q1<- subset (Q1, select = c(MINING_DW_SUBSCR_NO,Date,Time,churn_diff_day))
hist(Q1$churn_diff_day, main="Histogram for Days of Churn",
     xlab="churn_diff_day",
     border="blue",
     col="green",
     breaks=10)
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/dataata/Q1")
write.csv(Q1, "Q1.csv")

#Q2(Q3-1)  Q3-1  1/11~4/30 –F秈┍计㎝churn计 (ぃ珹ぱ计璽计ぃぱずchurn)rn)
# All_merge5<-read.csv("All_store_Data5.csv")
All_merge5_sub<- subset (All_merge5, as.Date(Date)>='2017-01-01' )
All_merge5_sub<- subset (All_merge5_sub, as.Date(Date)<='2017-04-30' )
# All_merge5_sub<- subset (All_merge5_sub, last_one==1 )
All_merge5_sub<- subset(All_merge5_sub, MINING_DW_SUBSCR_NO!=0)
All_merge5_sub<- subset(All_merge5_sub, MINING_DW_SUBSCR_NO!="?")
Q2<- subset (All_merge5_sub, as.numeric(churn_diff_day)>=0 | is.na(churn_diff_day))
Q2<- subset (All_merge5_sub, select = c(MINING_DW_SUBSCR_NO,Store_No, Store_Name, Store_Area, Date, Time,churn_or_not, churn_diff_day))
Q2_group_by <- Q2 %>% 
  group_by(Store_No,Store_Name,Store_Area,churn_or_not) %>% 
  summarise(count = n())

setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data/Q2")
write.csv(Q2_group_by, "Q2_group_by.csv")


# Q3 (Q3-2) Q3-2 –贺秈┍抖F 秈┍计のNP churn计 (ぱずchurn琌程Ω秈┍抖)?嗗??)
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data")
All_merge5<-read.csv("All_store_Data5.csv")
# All_merge5<-data.table::fread("All_store_Data5.csv", header = TRUE, sep="\t")
All_merge5_sub<- subset (All_merge5, as.Date(Date)>='2017-01-01' )
All_merge5_sub<- subset (All_merge5_sub, as.Date(Date)<='2017-04-30' )

# count the number of entering the store
# count <- All_merge5  %>% group_by(Subscriber_No) %>% summarise(count = n())
# All_merge5_sub <-merge(All_merge5_sub,count, by = c("Subscriber_No"), all.x = TRUE, all.y = FALSE)

# reflag churn in 7 days
All_merge5_sub <-
  All_merge5_sub %>% 
  mutate(churn_or_not = ifelse(churn_diff_day>0 & churn_diff_day<=7 ,1,0) )

All_merge5_sub = subset(All_merge5_sub, select = c(Subscriber_No, MINING_DW_SUBSCR_NO,Date,Time, group,Store_No,Store_Name,count, last_one, churn_or_not))
# All_merge4_sub = subset(All_merge4, MINING_DW_SUBSCR_NO!= 0, !is.na(MINING_DW_SUBSCR_NO))
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!=0)
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!="?")
All_merge5_sub = arrange(All_merge5_sub, Subscriber_No, Date)
All_merge5_sub$Date = as.Date(All_merge5_sub$Date )

##### F3R7C #####
# Split Data 
mydata_F = All_merge5_sub %>%
  filter(group=='F') %>%
  select(MINING_DW_SUBSCR_NO,Store_NoF=Store_No,Store_NameF= Store_Name, StoreF=group,DateF=Date, TimeF=Time, LastF=last_one, ChurnF=churn_or_not)

mydata_R = All_merge5_sub %>%
  filter(group=='R') %>%
  select(MINING_DW_SUBSCR_NO,Store_NoR=Store_No,Store_NameR= Store_Name, StoreR=group,DateR=Date, TimeR=Time, LastR=last_one, ChurnR=churn_or_not)

# Find 3 Day data 
mydata_3day = full_join(mydata_F,mydata_R,by="MINING_DW_SUBSCR_NO") %>%
  filter( (DateF <= DateR & DateF+3>=DateR & LastR==1))# | (DateR <= DateF & DateR+3>=DateF) )

# Final
mydata_both = bind_rows( mydata_3day %>% select(MINING_DW_SUBSCR_NO,Store_No=Store_NoF, Store_Name=Store_NameF,Date=DateF, Time=TimeF, group=StoreF,last=LastF, churn=ChurnF) ,
                         mydata_3day %>% select(MINING_DW_SUBSCR_NO,Store_No=Store_NoR, Store_Name=Store_NameR,Date=DateR, Time=TimeR, group=StoreR,last=LastR, churn=ChurnR)) %>% 
  distinct(MINING_DW_SUBSCR_NO, Store_No, Store_Name, Date, Time, group, last, churn)
mydata_both$Bad_Store = 1
mydata_both_F3R7C=mydata_both

mydata_both_F3R7C <-
  mydata_both_F3R7C %>% 
  mutate(churn = ifelse(is.na(churn) ,0,churn) )

setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/dataata/Q3")

write.csv(mydata_both_F3R7C, "mydata_both_F3R7C_v2.csv")

##### F3F7C #####
# Split Data 
mydata_F1 = All_merge5_sub %>%
  filter(group=='F') %>%
  select(MINING_DW_SUBSCR_NO,Store_NoF1=Store_No,Store_NameF1= Store_Name, StoreF1=group,DateF1=Date, LastF1=last_one, ChurnF1=churn_or_not, CountF1=count)

mydata_F2 = All_merge5_sub %>%
  filter(group=='F') %>%
  select(MINING_DW_SUBSCR_NO,Store_NoF2=Store_No,Store_NameF2= Store_Name, StoreF2=group,DateF2=Date, LastF2=last_one, ChurnF2=churn_or_not, CountF2=count)

# Find 3 Day data 
mydata_3day = full_join(mydata_F1,mydata_F2,by="MINING_DW_SUBSCR_NO") %>%
  filter( (DateF1 <= DateF2 & DateF1+3>=DateF2 & LastF2==1 & CountF1>1 & CountF2>1))# | (DateR <= DateF & DateR+3>=DateF) )

# Final
mydata_both = bind_rows( mydata_3day %>% select(MINING_DW_SUBSCR_NO,Store_No=Store_NoF1, Store_Name=Store_NameF1,Date=DateF1,group=StoreF1,last=LastF1, churn=ChurnF1, count=CountF1) ,
                         mydata_3day %>% select(MINING_DW_SUBSCR_NO,Store_No=Store_NoF2, Store_Name=Store_NameF2,Date=DateF2,group=StoreF2,last=LastF2, churn=ChurnF2, count=CountF2)) %>% 
  distinct(MINING_DW_SUBSCR_NO, Store_No, Store_Name, Date,group,last,churn,count)
mydata_both$Bad_Store = 1
mydata_both_F3F7C=mydata_both

mydata_both_F3F7C <-
  mydata_both_F3F7C %>% 
  mutate(churn = ifelse(is.na(churn) ,0,churn) )

write.csv(mydata_both_F3F7C, "mydata_both_F3F7C.csv")

##### 0F7C #####
# Split Data 
mydata_F1 = All_merge5_sub %>%
  filter(group=='F') %>%
  select(MINING_DW_SUBSCR_NO,Store_NoF1=Store_No,Store_NameF1= Store_Name, StoreF1=group,DateF1=Date, LastF1=last_one, ChurnF1=churn_or_not, CountF1=count)

mydata_F2 = All_merge5_sub %>%
  filter(group=='F') %>%
  select(MINING_DW_SUBSCR_NO,Store_NoF2=Store_No,Store_NameF2= Store_Name, StoreF2=group,DateF2=Date, LastF2=last_one, ChurnF2=churn_or_not, CountF2=count)

# Find 3 Day data 
mydata_3day = full_join(mydata_F1,mydata_F2,by="MINING_DW_SUBSCR_NO") %>%
  filter( (DateF1 <= DateF2 & DateF1+3>=DateF2 & LastF2==1 & CountF1==1 & CountF2==1))# | (DateR <= DateF & DateR+3>=DateF) )

# Final
mydata_both = bind_rows( mydata_3day %>% select(MINING_DW_SUBSCR_NO,Store_No=Store_NoF1, Store_Name=Store_NameF1,Date=DateF1,group=StoreF1,last=LastF1, churn=ChurnF1, count=CountF1) ,
                         mydata_3day %>% select(MINING_DW_SUBSCR_NO,Store_No=Store_NoF2, Store_Name=Store_NameF2,Date=DateF2,group=StoreF2,last=LastF2, churn=ChurnF2, count=CountF2)) %>% 
  distinct(MINING_DW_SUBSCR_NO, Store_No, Store_Name, Date,group,last,churn,count)
mydata_both$Bad_Store = 1
mydata_both_0F7C=mydata_both

mydata_both_0F7C <-
  mydata_both_0F7C %>% 
  mutate(churn = ifelse(is.na(churn) ,0,churn) )

write.csv(mydata_both_0F7C, "mydata_both_0F7C.csv")


# Q4 1/1~4/30 NP churn羆计
rm(list=ls())
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data")

All_merge5<-read.csv("All_store_Data5.csv")
All_merge5_sub<- subset (All_merge5, as.Date(Date)>='2017-01-01' )
All_merge5_sub<- subset (All_merge5_sub, as.Date(Date)<='2017-04-30' )
All_merge5_sub<- subset (All_merge5_sub, last_one==1 )
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!=0)
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!="?")
#?槸?惁??塩hurn (?柊澧炰?�娆勪??)
All_merge5_sub <-
  All_merge5_sub %>% 
  mutate(churn_or_not = ifelse(is.na(INACTV_DATE),0,1) )
All_merge5_sub_groupby <- All_merge5_sub %>% 
  group_by(churn_or_not) %>% 
  summarise(count = n())

setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/dataata/Q4")
write.csv(All_merge5_sub_groupby, "All_merge5_sub_groupby.csv")
 
# Q5 4/1~4/30 秈┍め秈┍ぇ戳计だchurn rate

#step 1 : 程掸秈┍ID
All_merge5<-read.csv("All_store_Data5.csv")
All_merge5_sub<- subset (All_merge5, as.Date(Date)>='2017-04-01' )
All_merge5_sub<- subset (All_merge5_sub, as.Date(Date)<='2017-04-30' )
All_merge5_sub<- subset (All_merge5_sub, last_one==1 )
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!=0)
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!="?")

Q5_Apr_Store_Mining = All_merge5_sub %>%
  select(MINING_DW_SUBSCR_NO)
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data/Q5")
write.csv(Q5_Apr_Store_Mining,"Q5_Apr_Store_Mining.csv")

#step 2 : ﹃秈┍赣る逞緇计勬暩
Apr_Store<- All_merge5_sub #%>%
Q5<- subset (Apr_Store, select = c(MINING_DW_SUBSCR_NO,Store_No, Store_Name, Store_Area, Date, Time,churn_or_not, churn_diff_day,group))

# SQLAExport_201704Store_201704<-read.table("SQLAExport_201704Store_201704.txt", header=TRUE, sep="\t")

SQLAExport_201704Store<-read.table("SQLAExport_201704Store.txt", header=TRUE, sep="\t")
# SQLAExport_201704Store <-subset(SQLAExport_201704Store, as.Date(DATA_MONTH)="2017-04-01")
All_merge <-merge(Q5,SQLAExport_201704Store, by = c("MINING_DW_SUBSCR_NO"), all.x = TRUE, all.y = FALSE)

# install.packages("zoo")
require(zoo)
All_merge$Date_store <- as.yearmon(as.Date(All_merge$Date), "%b %Y")
All_merge$Date_data= as.yearmon(as.Date(All_merge$DATA_MONTH), "%b %Y")
All_merge_month=subset(All_merge,Date_store==Date_data)

write.csv(Q5,"Q5.csv")
write.csv(All_merge_month,"All_merge_month_Apr.csv")


# Q5* 2/1~2/28秈┍め秈┍ぇ戳计だchurn rate
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data")
#step 1 : 程掸秈┍ID
All_merge5<-read.csv("All_store_Data5.csv")
All_merge5_sub<- subset (All_merge5, as.Date(Date)>='2017-02-01' )
All_merge5_sub<- subset (All_merge5_sub, as.Date(Date)<='2017-02-28' )
All_merge5_sub<- subset (All_merge5_sub, last_one==1 )
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!=0)
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!="?")

Q5_Feb_Store_Mining = All_merge5_sub %>%
  select(MINING_DW_SUBSCR_NO)
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data/Q5")
write.csv(Q5_Feb_Store_Mining,"Q5_Feb_Store_Mining.csv")

#step 2 : ﹃秈┍赣る逞緇计勬堢?勬暩
Feb_Store<- All_merge5_sub #%>%
Q5_<- subset (Feb_Store, select = c(MINING_DW_SUBSCR_NO,Store_No, Store_Name, Store_Area, Date, Time,churn_or_not, churn_diff_day, group))

SQLAExport_201702Store_201702_part1<-read.table("SQLAExport_201702Store_201702_part1.txt", header=TRUE, sep="\t")
SQLAExport_201702Store_201702_part2<-read.table("SQLAExport_201702Store_201702_part2.txt", header=TRUE, sep="\t")
SQLAExport_201702Store<-rbind(SQLAExport_201702Store_201702_part1,SQLAExport_201702Store_201702_part2)
# SQLAExport_201704Store <-subset(SQLAExport_201704Store, as.Date(DATA_MONTH)="2017-04-01")
All_merge <-merge(Q5_,SQLAExport_201702Store, by = c("MINING_DW_SUBSCR_NO"), all.x = TRUE, all.y = FALSE)

# install.packages("zoo")
require(zoo)
All_merge$Date_store <- as.yearmon(as.Date(All_merge$Date), "%b %Y")
All_merge$Date_data= as.yearmon(as.Date(All_merge$DATA_MONTH), "%b %Y")
All_merge_month=subset(All_merge,Date_store==Date_data)

write.csv(Q5_,"Q5_.csv")
write.csv(All_merge_month,"All_merge_month_Feb.csv")


# Q5** 3/1~3/31秈┍め秈┍ぇ戳计だchurn rate
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data")
#step 1 : 程掸秈┍ID
All_merge5<-read.csv("All_store_Data5.csv")
All_merge5_sub<- subset (All_merge5, as.Date(Date)>='2017-03-01' )
All_merge5_sub<- subset (All_merge5_sub, as.Date(Date)<='2017-03-31' )
All_merge5_sub<- subset (All_merge5_sub, last_one==1 )
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!=0)
All_merge5_sub = subset(All_merge5_sub, MINING_DW_SUBSCR_NO!="?")

Q5_Mar_Store_Mining = All_merge5_sub %>%
  select(MINING_DW_SUBSCR_NO)
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data/Q5")
write.csv(Q5_Mar_Store_Mining,"Q5_Mar_Store_Mining.csv")

#step 2 : ﹃秈┍赣る逞緇计勬暩
Mar_Store<- All_merge5_sub #%>%
Q5_<- subset (Mar_Store, select = c(MINING_DW_SUBSCR_NO,Store_No, Store_Name, Store_Area, Date, Time,churn_or_not, churn_diff_day, group))

SQLAExport_201703Store_201703_part1<-read.table("SQLAExport_201703Store_201703_part1.txt", header=TRUE, sep="\t")
SQLAExport_201703Store_201703_part2<-read.table("SQLAExport_201703Store_201703_part2.txt", header=TRUE, sep="\t")
SQLAExport_201703Store<-rbind(SQLAExport_201703Store_201703_part1,SQLAExport_201703Store_201703_part2)
# SQLAExport_201704Store <-subset(SQLAExport_201704Store, as.Date(DATA_MONTH)="2017-04-01")
All_merge <-merge(Q5_,SQLAExport_201703Store, by = c("MINING_DW_SUBSCR_NO"), all.x = TRUE, all.y = FALSE)

install.packages("zoo")
require(zoo)
All_merge$Date_store <- as.yearmon(as.Date(All_merge$Date), "%b %Y")
All_merge$Date_data= as.yearmon(as.Date(All_merge$DATA_MONTH), "%b %Y")
All_merge_month=subset(All_merge,Date_store==Date_data)

# write.csv(Q5_,"Q5_.csv")
write.csv(All_merge_month,"All_merge_month_Mar.csv")

#Q6 1/11~6/24拟絏瞒秨め临琩(钵盽)
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data")
All_merge5<-read.csv("All_store_Data5.csv")

All_merge5_minuschurn<-subset(All_merge5, All_merge5$churn_diff_day<0)
# All_merge5_minuschurn_v<-subset(All_merge5_minuschurn, All_merge5_minuschurn$Store_Name=="??熷?庤?曟?戜?屽?犵?熼?�甯?")

hist(All_merge5_minuschurn$churn_diff_day, main="Histogram for Days of Churn",
     xlab="churn_diff_day",
     border="blue",
     col="green",
     breaks=100)

#number of customers
All_merge5_minuschurn_groupby_distinct <- All_merge5_minuschurn %>% 
  group_by() %>% 
  summarise(count = n_distinct(MINING_DW_SUBSCR_NO))


All_merge5_minuschurn_groupby <- All_merge5_minuschurn %>% 
  group_by(Store_Name,as.yearmon(Date)) %>% 
  summarise(count = n())
setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data/Q6")
write.csv(All_merge5_minuschurn_groupby,"All_merge5_minuschurn_groupby.csv")

All_merge5_minuschurn_groupby_distinct <- All_merge5_minuschurn %>% 
  group_by(Store_Name,as.yearmon(Date)) %>% 
  summarise(count = n_distinct(MINING_DW_SUBSCR_NO))
write.csv(All_merge5_minuschurn_groupby_distinct,"All_merge5_minuschurn_groupby_distinct.csv")

setwd("D:/Dana/4. Analysis/10. 秈┍琩 part 2/data/Q5")
data<-read.csv("All_merge_month_Mar.csv")
#practice draw
install.packages("reshape")
library(reshape)

cast(data, DATA_MONTH ~ churn_or_not)
