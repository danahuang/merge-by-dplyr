#----------------------------------
# ²MÅÜ¼Æ¸ê®Æ,«ü©w¸ê®Æ§¨¦ì¸m
#----------------------------------
rm(list=ls())
dir<-setwd("D:/Dana/4. Analysis/10. ¶i©±¬d¦X¬ù part 2/data")

library(data.table)
library(dplyr)
library(reshape2)


#----------------------------------
# ¶×¤J¸ê®Æ
#----------------------------------

#¶×¤J¨C¤ë¶i©±¸ê®Æ

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
# ·s¼WÄæ¦ì
#----------------------------------


#°µF©MRÄæ¦ì
category <- function(x) if(nchar(x) == 7) "F" else if (nchar(x) == 4) "R" else "Error"
All$group <- sapply(All$Store_No,category)

#«ö«È¤áID¤Î¶i©±®É¶¡±Æ¦C??
All_sorted <- arrange(All, Subscriber_No, Date, Time)

# ­pºâ«È¤á¶i©±¦¸¼Æ
library(dplyr)
count <- All_sorted  %>% group_by(Subscriber_No) %>% summarise(count = n())
All_merge <-merge(All_sorted,count, by = c("Subscriber_No"), all.x = TRUE, all.y = FALSE)

# ¬O§_¦³¶iF©ÎR (·s¼W¤GÄæ¦ì))
FR <-dcast(All_merge,formula=Subscriber_No~group, value.var="count")
All_merge2 <-merge(All_merge,FR, by = c("Subscriber_No"), all.x = TRUE, all.y = FALSE)

# ¬O§_¦³¶iF¥BR (·s¼W¤@Äæ¦ì)
All_merge2 <-
  All_merge2 %>% 
  mutate(F_and_R = All_merge2$F*All_merge2$R )

#¶×¤JNP churn¤é´Á©M¦X¬ù¨ì´Á¤ë¼Æ?„åˆ°??Ÿæ?ˆæ•¸
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

#©M¤U¤@µ§ªº¤Ñ¼Æ®t (·s¼W¤@Äæ¦ì)
All_merge3<-All_merge3 %>% 
  tbl_df() %>% 
  mutate(lead_date = lead(Date),
         diff_day = as.Date(lead_date) - as.Date(Date))

#¸Óµ§¶i©±¤é©Mchurn¤é´Áªº¤Ñ¼Æ®t (·s¼W¤@Äæ¦ì)
All_merge3$churn_diff_day<-as.Date(All_merge3$INACTV_DATE) - as.Date(All_merge3$Date)

#¬O§_¦³churn (·s¼W¤@Äæ¦ì)
All_merge3 <-
  All_merge3 %>% 
  mutate(churn_or_not = ifelse(is.na(INACTV_DATE),0,1) )

#¬O§_¬°¸Ó«È¤á³Ì«á¤@µ§ (·s¼W¤@Äæ¦ì)
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

# All_merge5_sub_Store_Name_??†æ?_1 <- All_merge5_sub_Store_Name_??†æ?? %>% 
#   group_by(Store_Name) %>% 
#   summarise(count_M = n())

All_merge <-merge(All_merge5_sub_Store_Name,All_merge5_sub_Store_Name_M, by = c("Store_Name","Store_No"), all.x = TRUE, all.y = FALSE)
write.csv(All_merge,"15days_F_List_Y17May.csv")
