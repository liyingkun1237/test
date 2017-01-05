
################################public_function.R#################
#public_function.R
options(digits=4)
library(dplyr)
library(entropy)
library(lubridate)
library(stringr)
library(parallel)

library(tidyr)
library(RCurl)
library(rjson)
# 描述性统计量
cv <- function(x) { x <- na.omit(x); sd(x)/mean(x) }

range <- function(x){ x <- na.omit(x); max(x)-min(x)}

skew <-  function(x){
  x <- na.omit(x)
  if(length(x)==0) return(NA)
  n<-length(x)
  m<-mean(x)
  s<-sd(x)

  if(n<=2|is.na(s)|s==0) return(NA)
  Skewness <- n/((n-1)*(n-2))*sum((x-m)^3)/s^3
  return(Skewness)
}

kurto <-  function(x){
  x <- na.omit(x)
  x <- na.omit(x)
  if(length(x)==0) return(NA)
  n<-length(x)
  m<-mean(x)
  s<-sd(x)
  if(n<=3||is.na(s)|s==0) return(NA)
  Kurtosis <-((n*(n-1))/((n-1)*(n-2)*(n-3))*sum((x-m)^4)/s^4-(3*(n-1)^2)/((n-2)*(n-3)))
  return(Kurtosis)
}


ql <- function(x){ x <- na.omit(x);fivenum(x)[2]}
qu <- function(x){ x <- na.omit(x);fivenum(x)[4]}
qd <- function(x){ x <- na.omit(x);fivenum(x)[4]-fivenum(x)[2]}

q1 <- function(x){ x <- na.omit(x);quantile(x,probs=0.1)}
q2 <- function(x){ x <- na.omit(x);quantile(x,probs=0.2)}
q3 <- function(x){ x <- na.omit(x);quantile(x,probs=0.3)}
q4 <- function(x){ x <- na.omit(x);quantile(x,probs=0.4)}
q5 <- function(x){ x <- na.omit(x);quantile(x,probs=0.5)}
q6 <- function(x){ x <- na.omit(x);quantile(x,probs=0.6)}
q7 <- function(x){ x <- na.omit(x);quantile(x,probs=0.7)}
q8 <- function(x){ x <- na.omit(x);quantile(x,probs=0.8)}
q9 <- function(x){ x <- na.omit(x);quantile(x,probs=0.9)}

normentropy <-  function(x){x <- na.omit(x);  n<-sum(x);    return(entropy(x)/log(n))}

nd <- function(x){n_distinct( na.omit(x))}

#############二元函数#########################################################################

minus <- function(p,q){p-q}
divide <- function(p,q){p/q}
difrate <- function(p,q){(p-q)/q}

GDDIFF <-c('minus','divide','difrate')

desc.count <- funs(nd,n(),sum(.,na.rm = T),mean(.,na.rm = T),min(.,na.rm = T),max(.,na.rm = T),median(.,na.rm = T),sd(.,na.rm = T),var(.,na.rm = T),cv,range,skew,kurto,qu,ql,qd,entropy,normentropy)
gdcnt <- names(desc.count)
desc.count.indexes <- 3:(length(gdcnt)+2)

desc.amt <- funs(sum(.,na.rm = T),mean(.,na.rm = T),min(.,na.rm = T),max(.,na.rm = T),median(.,na.rm = T),sd(.,na.rm = T),var(.,na.rm = T),cv,range,skew,kurto,qu,ql,qd)
gdamt <- names(desc.amt)
desc.amt.indexes <- 3:(length(gdamt)+2)

desc.bincount <- funs(sd(.,na.rm = T),var(.,na.rm = T),cv,range,skew,kurto,qd,entropy,normentropy)
gdbct <- names(desc.bincount)
desc.bincount.indexes <- 3:(length(gdbct)+2)

desc.binamt <- funs(sd(.,na.rm = T),var(.,na.rm = T),cv,range,skew,kurto,qd)
gdbamt <- names(desc.binamt)
desc.binamt.indexes <- 3:(length(gdbamt)+2)

edesc.binamt <- funs(sum(.,na.rm = T),mean(.,na.rm = T),min(.,na.rm = T),max(.,na.rm = T),median(.,na.rm = T),sd(.,na.rm = T),var(.,na.rm = T),cv,range,skew,kurto,qu,ql,qd,q1,q2,q3,q4,q5,q6,q7,q8,q9)

########分变量组计算变量的  函数名 也即 变量组核心名（具体变量组需要加时间后缀 以 '.'分割）#############################################################

vargroup_fuc_name <- c('fdesc_hourbin_amtaorder','tsdesc_amtorderdiff','gdesc_order_product')

observe_interval_mark  <- c('02w','01m','03m','06m','01y','all')
observe_interval_byday <- c(  14 ,  30 ,  90 , 180 , 360 ,Inf)

names(observe_interval_byday) <- observe_interval_mark
#--------------------------------------------------------------#






##########################gdesc_order_product.R###############
gdesc_order_product <- function(order_product,obs_interval)
{
  #1208,修改 用于金额的划分，只在这里用到，由public_function.R迁移过来
  amtCutPoint <- c(0,50,100,200,300,500,1000,2000,3000,5000,15000,Inf)

  order_product_observed <- order_product%>%filter(difftime(appl_sbm_tm,time_order,units = "days")<=obs_interval)
  ################多次用到的数据集准备###################################################################################
  order_product_observed_all_row <- order_product_observed%>%select(appl_no,custorm_id,no_order,product_id)
  order_product_observed_all_row_s <- order_product_observed%>%select(appl_no,custorm_id,no_order,sts_order,product_id)%>%filter(sts_order%in%c('完成','充值成功'))
  order_product_observed_all_row_c <- order_product_observed%>%select(appl_no,custorm_id,no_order,sts_order,product_id)%>%filter(sts_order%in%c('已取消','订单取消','已取消订单','配送退货'))
  #################按不同算法分组计算变量###############################################################################

  cust_gdesc_aorder_cntproduct <- order_product_observed_all_row%>%
    group_by(appl_no,custorm_id,no_order)%>% summarise(num_product = n())%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_product)
  names(cust_gdesc_aorder_cntproduct)[desc.count.indexes] <- paste0('cust_gdesc_',gdcnt,'_aorder_cntproduct')

  cust_gdesc_sorder_cntproduct <- order_product_observed_all_row_s%>%
    group_by(appl_no,custorm_id,no_order)%>% summarise(num_product = n())%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_product)
  names(cust_gdesc_sorder_cntproduct)[desc.count.indexes] <- paste0('cust_gdesc_',gdcnt,'_sorder_cntproduct')

  cust_gdesc_corder_cntproduct <- order_product_observed_all_row_c%>%
    group_by(appl_no,custorm_id,no_order)%>% summarise(num_product = n())%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_product)
  names(cust_gdesc_corder_cntproduct)[desc.count.indexes] <- paste0('cust_gdesc_',gdcnt,'_corder_cntproduct')
  #######################################################################################################
  cust_gdesc_product_cntaorder<- order_product_observed_all_row%>%
    filter(!is.na(product_id))%>%
    group_by(appl_no,custorm_id,product_id)%>% summarise(num_orders = n())%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_orders)
  names(cust_gdesc_product_cntaorder)[desc.count.indexes] <- paste0('cust_gdesc_',gdcnt,'_product_cntaorder')

  cust_gdesc_product_cntsorder<-
    order_product_observed_all_row_s%>%select(appl_no,custorm_id,product_id,no_order)%>%na.omit() %>%
    group_by(appl_no,custorm_id,product_id)%>%summarise(num_orders=n())%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_orders)
  names(cust_gdesc_product_cntsorder)[desc.count.indexes] <- paste0('cust_gdesc_',gdcnt,'_product_cntsorder')

  cust_gdesc_product_cntcorder<-
    order_product_observed_all_row_c%>%select(appl_no,custorm_id,product_id,no_order)%>%na.omit() %>%
    group_by(appl_no,custorm_id,product_id)%>%summarise(num_orders=n())%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_orders)
  names(cust_gdesc_product_cntcorder)[desc.count.indexes] <- paste0('cust_gdesc_',gdcnt,'_product_cntcorder')
  ######## 金额 #######################################################################################


  ###数据集准备

  order_observed <- order_product_observed%>%select(appl_no,custorm_id,no_order,sts_order,amt_order)%>%distinct()%>%  mutate(amt_order_bined=cut(amt_order,breaks = amtCutPoint))

  order_observed_s <-   order_observed%>%filter(sts_order%in%c('完成','充值成功'))
  order_observed_c <-   order_observed%>%filter(sts_order%in%c('已取消','订单取消','已取消订单','配送退货'))
  ######################################################################################################
  ### 金额变量计算
  cust_gdesc_aorder_amtaorder <-
    order_observed%>%group_by(appl_no,custorm_id)%>%
    summarise_each(desc.amt,amt_order)
  names(cust_gdesc_aorder_amtaorder)[desc.amt.indexes] <- paste0('cust_gdesc_',gdamt,'_aorder_amtaorder')

  cust_gdesc_sorder_amtaorder <-
    order_observed_s%>%select(appl_no,custorm_id,amt_order)%>%group_by(appl_no,custorm_id)%>%
    summarise_each(desc.amt,amt_order)
  names(cust_gdesc_sorder_amtaorder)[desc.amt.indexes] <- paste0('cust_gdesc_',gdamt,'_sorder_amtaorder')

  cust_gdesc_corder_amtaorder <-
    order_observed_c%>%select(appl_no,custorm_id,amt_order)%>%group_by(appl_no,custorm_id)%>%
    summarise_each(desc.amt,amt_order)
  names(cust_gdesc_corder_amtaorder)[desc.amt.indexes] <- paste0('cust_gdesc_',gdamt,'_corder_amtaorder')
  #########金额分组#################################################
  #### 金额分组计数##############
  cust_gdesc_amtbin_cntaorder <-
    order_observed%>%group_by(appl_no,custorm_id,amt_order_bined)%>%summarise(num_amtbin=n())%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.bincount,num_amtbin)
  names(cust_gdesc_amtbin_cntaorder)[desc.bincount.indexes] <- paste0('cust_gdesc_',gdbct,'_amtbin_cntaorder')

  cust_gdesc_amtbin_cntsorder <-
    order_observed_s%>%group_by(appl_no,custorm_id,amt_order_bined)%>%summarise(num_amtbin=n())%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.bincount,num_amtbin)
  names(cust_gdesc_amtbin_cntsorder)[desc.bincount.indexes] <- paste0('cust_gdesc_',gdbct,'_amtbin_cntsorder')

  cust_gdesc_amtbin_cntcorder <-
    order_observed_c%>%group_by(appl_no,custorm_id,amt_order_bined)%>%summarise(num_amtbin=n())%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.bincount,num_amtbin)
  names(cust_gdesc_amtbin_cntcorder)[desc.bincount.indexes] <- paste0('cust_gdesc_',gdbct,'_amtbin_cntcorder')
  #### 金额分组算总金额########
  cust_gdesc_amtbin_amtaorder <-
    order_observed%>%group_by(appl_no,custorm_id,amt_order_bined)%>%summarise(amt_amtbin=sum(amt_order))%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.bincount,amt_amtbin)
  names(cust_gdesc_amtbin_amtaorder)[desc.bincount.indexes] <- paste0('cust_gdesc_',gdbct,'_amtbin_amtaorder')

  cust_gdesc_amtbin_amtsorder <-
    order_observed_s%>%group_by(appl_no,custorm_id,amt_order_bined)%>%summarise(amt_amtbin=sum(amt_order))%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.bincount,amt_amtbin)
  names(cust_gdesc_amtbin_amtsorder)[desc.bincount.indexes] <- paste0('cust_gdesc_',gdbct,'_amtbin_amtsorder')

  cust_gdesc_amtbin_amtcorder <-
    order_observed_c%>%group_by(appl_no,custorm_id,amt_order_bined)%>%summarise(amt_amtbin=sum(amt_order))%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.bincount,amt_amtbin)
  names(cust_gdesc_amtbin_amtcorder)[desc.bincount.indexes] <- paste0('cust_gdesc_',gdbct,'_amtbin_amtcorder')

  ##### caculate phone/receive_addr related variables.
  if(obs_interval%in%c(90,180,360,Inf)){

    order_product_observed_phone_addr <- order_product_observed%>%
      select(appl_no,custorm_id,no_order,phone,amt_order, add_rec,name_rec)%>%unique()

    # force type of amt_order as numeric.
    order_product_observed_phone_addr$amt_order <- as.numeric(order_product_observed_phone_addr$amt_order)

    ## parse receive_addr into [rec_addr_province, rec_addr_city]
    # parse rec_addr_province
    province_end <- ifelse(str_sub(order_product_observed_phone_addr$add_rec,1,2)%in%c('内蒙','黑龙','钓鱼') ,1,0)+2
    order_product_observed_phone_addr$rec_addr_province <- str_sub(order_product_observed_phone_addr$add_rec,1,province_end)

    # parse rec_addr_city
    order_product_observed_phone_addr$rec_addr_city <- str_sub(order_product_observed_phone_addr$add_rec,province_end+1,str_locate(order_product_observed_phone_addr$add_rec,'[市|区|州]')[,1])

    # re-process the  province-level Municipality:   forcd rec_addr_city = rec_addr_province
    order_product_observed_phone_addr$rec_addr_city[order_product_observed_phone_addr$rec_addr_province=='北京'] <-'北京'
    order_product_observed_phone_addr$rec_addr_city[order_product_observed_phone_addr$rec_addr_province=='上海'] <-'上海'
    order_product_observed_phone_addr$rec_addr_city[order_product_observed_phone_addr$rec_addr_province=='重庆'] <-'重庆'
    order_product_observed_phone_addr$rec_addr_city[order_product_observed_phone_addr$rec_addr_province=='天津'] <-'天津'

    # 1.group:phone, aggre:no_order
    cust_gdesc_phone_cntaorder <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,phone)%>%summarise(num_order = n())%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_order)
    names(cust_gdesc_phone_cntaorder)[3:length(names(cust_gdesc_phone_cntaorder))] <- paste0('cust_gdesc_',names(desc.count),'_phone_cntorder')

    # 2.group:phone, aggre: rec_addr_province
    cust_gdesc_phone_cntaddrprovince <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,phone)%>%summarise(num_province = nd(rec_addr_province))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_province)
    names(cust_gdesc_phone_cntaddrprovince)[3:length(names(cust_gdesc_phone_cntaddrprovince))] <- paste0('cust_gdesc_',names(desc.count),'_phone_cntaddrprovince')

    # 3.group:phone, aggre: rec_addr_city
    cust_gdesc_phone_cntaddrcity <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,phone)%>%summarise(num_city = nd(rec_addr_city))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_city)
    names(cust_gdesc_phone_cntaddrcity)[3:length(names(cust_gdesc_phone_cntaddrcity))] <- paste0('cust_gdesc_',names(desc.count),'_phone_cntaddrcity')

    # 4. group:phone, aggre: name_rec
    cust_gdesc_phone_cntrecname <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,phone)%>%summarise(num_recname = nd(name_rec))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_recname)
    names(cust_gdesc_phone_cntrecname)[3:length(names(cust_gdesc_phone_cntrecname))] <- paste0('cust_gdesc_',names(desc.count),'_phone_cntrecname')


    # 5.group: phone, aggre:amt_order
    cust_gdesc_phone_descamt <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,phone)%>%summarise_each(funs(sum(., na.rm=T),mean(., na.rm=T)), amt_order)%>%rename(., sumamt=sum, avgamt=mean)%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.amt,c(sumamt,avgamt))

    names(cust_gdesc_phone_descamt)[3:length(names(cust_gdesc_phone_descamt))] <- paste0('cust_gdesc_',rep(names(desc.amt),each=2),'_phone_',rep(c('sumamt','avgamt'),length(desc.amt)))

    # 6.group: rec_addr_province,  aggre: no_order
    cust_gdesc_recaddrprovince_cntaorder <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,rec_addr_province)%>%summarise(num_order = n())%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_order)

    names(cust_gdesc_recaddrprovince_cntaorder)[3:length(names(cust_gdesc_recaddrprovince_cntaorder))] <- paste0('cust_gdesc_',names(desc.count),'_recaddrprovince_cntorder')

    # 7.group: rec_addr_city,  aggre: no_order
    cust_gdesc_recaddrcity_cntaorder <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,rec_addr_city)%>%summarise(num_order = n())%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_order)

    names(cust_gdesc_recaddrcity_cntaorder)[3:length(names(cust_gdesc_recaddrcity_cntaorder))] <- paste0('cust_gdesc_',names(desc.count),'_recaddrcity_cntorder')

    # 8.group: rec_addr_province, aggre: phone
    cust_gdesc_recaddrprovince_cntphone <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,rec_addr_province)%>%summarise(num_phone = nd(phone))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_phone)

    names(cust_gdesc_recaddrprovince_cntphone)[3:length(names(cust_gdesc_recaddrprovince_cntphone))] <- paste0('cust_gdesc_',names(desc.count),'_recaddrprovince_cntphone')

    # 9.group: rec_addr_city, aggre: phone
    cust_gdesc_recaddrcity_cntphone <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,rec_addr_city)%>%summarise(num_phone = nd(phone))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_phone)

    names(cust_gdesc_recaddrcity_cntphone)[desc.count.indexes] <- paste0('cust_gdesc_',names(desc.count),'_recaddrcity_cntphone')

    # 10.group: rec_addr_province, aggre: name_rec
    cust_gdesc_recaddrprovince_cntrecname <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,rec_addr_province)%>%summarise(num_recname = nd(name_rec))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_recname)

    names(cust_gdesc_recaddrprovince_cntrecname)[3:length(names(cust_gdesc_recaddrprovince_cntrecname))] <- paste0('cust_gdesc_',names(desc.count),'_recaddrprovince_cntrecname')

    # 11.group: rec_addr_city, aggre: name_rec
    cust_gdesc_recaddrcity_cntrecname <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,rec_addr_city)%>%summarise(num_recname = nd(name_rec))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_recname)

    names(cust_gdesc_recaddrcity_cntrecname)[3:length(names(cust_gdesc_recaddrcity_cntrecname))] <- paste0('cust_gdesc_',names(desc.count),'_recaddrcity_cntrecname')


    # 12.group: rec_addr_province, aggre: amt_order
    cust_gdesc_recaddrprovince_descamt <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,rec_addr_province)%>%summarise_each(funs(sum(., na.rm=T),mean(., na.rm=T)), amt_order)%>%rename(., sumamt=sum, avgamt=mean)%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.amt,c(sumamt,avgamt))

    names(cust_gdesc_recaddrprovince_descamt)[3:length(names(cust_gdesc_recaddrprovince_descamt))] <- paste0('cust_gdesc_',rep(names(desc.amt),each=2),'_recaddrprovince_',rep(c('sumamt','avgamt'),length(desc.amt)))

    # 13.group: rec_addr_city, aggre: amt_order
    cust_gdesc_recaddrcity_descamt <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,rec_addr_city)%>%summarise_each(funs(sum(., na.rm=T),mean(., na.rm=T)), amt_order)%>%rename(., sumamt=sum, avgamt=mean)%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.amt,c(sumamt,avgamt))

    names(cust_gdesc_recaddrcity_descamt)[3:length(names(cust_gdesc_recaddrcity_descamt))] <- paste0('cust_gdesc_',rep(names(desc.amt),each=2),'_recaddrcity_',rep(c('sumamt','avgamt'),length(desc.amt)))

    # 14.group:name_rec, aggre: order_cnt,
    cust_gdesc_recname_cntaorder <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,name_rec)%>%summarise(num_order = n())%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_order)
    names(cust_gdesc_recname_cntaorder)[3:length(names(cust_gdesc_recname_cntaorder))] <- paste0('cust_gdesc_',names(desc.count),'_recname_cntaorder')

    # 15.group:name_rec, aggre: rec_addr_province,
    cust_gdesc_recname_cntaddrprovince <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,name_rec)%>%summarise(num_province = nd(rec_addr_province))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_province)
    names(cust_gdesc_recname_cntaddrprovince)[3:length(names(cust_gdesc_recname_cntaddrprovince))] <- paste0('cust_gdesc_',names(desc.count),'_recname_cntaddrprovince')

    # 16.group:name_rec, aggre: rec_addr_city
    cust_gdesc_recname_cntaddrcity <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,name_rec)%>%summarise(num_city = nd(rec_addr_city))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_city)
    names(cust_gdesc_recname_cntaddrcity)[3:length(names(cust_gdesc_recname_cntaddrcity))] <- paste0('cust_gdesc_',names(desc.count),'_recname_cntaddrcity')

    # 17. group:name_rec, aggre: phone
    cust_gdesc_recname_cntphone <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,name_rec)%>%summarise(num_phone = nd(phone))%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.count,num_phone)
    names(cust_gdesc_recname_cntphone)[3:length(names(cust_gdesc_recname_cntphone))] <- paste0('cust_gdesc_',names(desc.count),'_recname_cntphone')

    # 18.group: name_rec, aggre:amt_order
    cust_gdesc_recname_descamt <- order_product_observed_phone_addr %>%
      group_by(appl_no,custorm_id,name_rec)%>%summarise_each(funs(sum(., na.rm=T),mean(., na.rm=T)), amt_order)%>%rename(., sumamt=sum, avgamt=mean)%>%
      group_by(appl_no,custorm_id)%>%summarise_each(desc.amt,c(sumamt,avgamt))

    names(cust_gdesc_recname_descamt)[3:length(names(cust_gdesc_recname_descamt))] <- paste0('cust_gdesc_',rep(names(desc.amt),each=2),'_recname_',rep(c('sumamt','avgamt'),length(desc.amt)))

  } #if-end.

  #############################################################################################################################

  if(obs_interval%in%c(90,180,360,Inf)){
    order_product_vargroup_list <- list(
      cust_gdesc_aorder_cntproduct,
      cust_gdesc_sorder_cntproduct,
      cust_gdesc_corder_cntproduct,
      cust_gdesc_product_cntaorder,
      cust_gdesc_product_cntsorder,
      cust_gdesc_product_cntcorder,
      cust_gdesc_aorder_amtaorder,
      cust_gdesc_sorder_amtaorder,
      cust_gdesc_corder_amtaorder,
      cust_gdesc_amtbin_cntaorder,
      cust_gdesc_amtbin_cntsorder,
      cust_gdesc_amtbin_cntcorder,
      cust_gdesc_amtbin_amtaorder,
      cust_gdesc_amtbin_amtsorder,
      cust_gdesc_amtbin_amtcorder,
      cust_gdesc_phone_cntaorder,
      cust_gdesc_phone_cntaddrprovince,
      cust_gdesc_phone_cntaddrcity,
      cust_gdesc_phone_cntrecname,
      cust_gdesc_phone_descamt,
      cust_gdesc_recaddrprovince_cntaorder,
      cust_gdesc_recaddrcity_cntaorder,
      cust_gdesc_recaddrprovince_cntphone,
      cust_gdesc_recaddrcity_cntphone,
      cust_gdesc_recaddrprovince_cntrecname,
      cust_gdesc_recaddrcity_cntrecname,
      cust_gdesc_recaddrprovince_descamt,
      cust_gdesc_recaddrcity_descamt,
      cust_gdesc_recname_cntaorder,
      cust_gdesc_recname_cntaddrprovince,
      cust_gdesc_recname_cntaddrcity,
      cust_gdesc_recname_cntphone,
      cust_gdesc_recname_descamt
    )
  }else{
    order_product_vargroup_list <- list(
      cust_gdesc_aorder_cntproduct,
      cust_gdesc_sorder_cntproduct,
      cust_gdesc_corder_cntproduct,
      cust_gdesc_product_cntaorder,
      cust_gdesc_product_cntsorder,
      cust_gdesc_product_cntcorder,
      cust_gdesc_aorder_amtaorder,
      cust_gdesc_sorder_amtaorder,
      cust_gdesc_corder_amtaorder,
      cust_gdesc_amtbin_cntaorder,
      cust_gdesc_amtbin_cntsorder,
      cust_gdesc_amtbin_cntcorder,
      cust_gdesc_amtbin_amtaorder,
      cust_gdesc_amtbin_amtsorder,
      cust_gdesc_amtbin_amtcorder
    )
  }


  order_product_vargroup <- Reduce(full_join,order_product_vargroup_list)

  ###############################################################################################################################
  # 构造函数及其输入参数列表
  vars_Xorder <- c(paste0(gdcnt,'_','Xorder_cntproduct'),paste0(gdcnt,'_product_cnt','Xorder'),paste0(gdamt,'_','Xorder_amtaorder'),paste0(gdbct,'_amtbin_cnt','Xorder'),paste0(gdbct,'_amtbin_amt','Xorder'))
  vars_aorder <- paste0('cust_gdesc_',gsub(pattern = "Xorder", replacement = "aorder", vars_Xorder))
  vars_sorder <- paste0('cust_gdesc_',gsub(pattern = "Xorder", replacement = "sorder", vars_Xorder))
  vars_corder <- paste0('cust_gdesc_',gsub(pattern = "Xorder", replacement = "corder", vars_Xorder))
  # 函数入参列表 变量1 变量2
  vars_input1 <- c(vars_sorder,vars_corder,vars_corder)
  vars_input2 <- c(vars_aorder,vars_aorder,vars_sorder)
  # 构造 衍生变量 变量名
  # 1、衍生变量名模式 （包含 分组差异函数 作为 命名变元）
  gd <- sapply(str_extract_all(vars_input1,'[a-zA-Z]+'), function(x)x[3])
  pmqm <- paste0(gsub('order','',sapply(str_extract_all(vars_input1,'[asc]order'), function(x)x[1])),gsub('order','',sapply(str_extract_all(vars_input2,'[asc]order'), function(x)x[1])) )
  kv <- sapply(str_extract_all(vars_input1,'[a-zA-Z]+'), function(x) paste0(x[4],'_',x[5]))
  kv <- str_replace_all(kv,'[sc]order','aorder') # 由于我们目前 只对订单状态求子集
  PMQM_GD_KV <- paste0(pmqm,'_',gd,'_',kv)
  der_name <- paste0('cust_gddif_','GDDIFF','_',PMQM_GD_KV)
  # 2、为 分组差异函数名 变元 赋值  已前置

  # 3、构造 衍生变量 变量名    der_name_vec
  eval(parse(text=paste0('der_name_',GDDIFF,"<- str_replace_all(der_name,'GDDIFF' ,","'",GDDIFF,"'",')')))
  der_name_group <- paste0('der_name_',GDDIFF)
  eval(parse(text=paste0('der_name_',GDDIFF,"<- str_replace_all(der_name,'GDDIFF' ,","'",GDDIFF,"'",')')))
  # der_name_group <- paste0('der_name_',GDDIFF)
  eval(parse(text=paste0('der_name_vec <- c(',paste0(der_name_group,collapse = ','),')')))

  # 构造分组差异函数 入参（输入变量1，输入变量2） 和 出参 （衍生变量名）
  # 需要构造列表（衍生变量名，分组差异函数名，输入变量1，输入变量2）  der_element
  varinput1 <- rep(vars_input1,length(GDDIFF))
  varinput2 <- rep(vars_input2,length(GDDIFF))
  funcname <- rep(GDDIFF, times = length(vars_input1),each = 1)
  der_element <- cbind(varinput1,varinput2,der_name_vec,funcname)
  #计算 衍生变量
  der_var_logic <- paste0(der_element[,'der_name_vec']," = ", funcname,"(",der_element[,'varinput1'],",",der_element[,'varinput2'],")")
  text <- paste0("order_product_vargroup01 <-order_product_vargroup%>%mutate(",paste0(der_var_logic,collapse = ','),")")
  eval(parse(text=text))

  code.rename <- paste0("names(order_product_vargroup01) <-  str_replace(names(order_product_vargroup01),pattern = 'cust_',replacement = 'cust_",names(which(observe_interval_byday==obs_interval)),"_')",collapse = ";")

  eval(parse(text = code.rename))

  order_product_vargroup01 }

#test
#gdesc_order_product(df_order_product%>%head(10),14)

#---------------------------------------------------------------------#









########################fdesc_hourbin_amtaorder.R#####################

fdesc_hourbin_amtaorder <- function(order_product,obs_interval){
  #1208,修改 用于时间的划分，只在这里用到，由public_function.R迁移过来
  hourCutPoint <- c(-1,  5 , 9 ,12 ,14, 18, 20,22 )

  order_product_observed <- order_product%>%filter(difftime(appl_sbm_tm,time_order,units = "days")<=obs_interval)
  ########################数据及构造########################################################################
  order_observed_hourcut <- order_product_observed%>%select(appl_no,custorm_id,no_order,amt_order,time_order)%>%distinct()%>%
    mutate(hour_order=hour(time_order),hourtrans_order=as.numeric(ifelse(hour_order==23,-1,hour_order)),hour_order_bin=cut(hourtrans_order,breaks = hourCutPoint,right=F,include.lowest = T))%>%
    select(appl_no,custorm_id,hour_order_bin,amt_order)
  ########################数据及准备##########################################################################
  order_observed_hourcut_a <- order_observed_hourcut
  order_observed_hourcut_1 <- order_observed_hourcut%>%filter(hour_order_bin=="[-1,5)")
  order_observed_hourcut_2 <- order_observed_hourcut%>%filter(hour_order_bin=="[5,9)")
  order_observed_hourcut_3 <- order_observed_hourcut%>%filter(hour_order_bin=="[9,12)")
  order_observed_hourcut_4 <- order_observed_hourcut%>%filter(hour_order_bin=="[12,14)")
  order_observed_hourcut_5 <- order_observed_hourcut%>%filter(hour_order_bin=="[14,18)")
  order_observed_hourcut_6 <- order_observed_hourcut%>%filter(hour_order_bin=="[18,20)")
  order_observed_hourcut_7 <- order_observed_hourcut%>%filter(hour_order_bin=="[20,22]")
  # a
  cust_fdesc_ahourbin_amtaorder <- order_observed_hourcut_a%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.binamt,amt_order)
  names(cust_fdesc_ahourbin_amtaorder)[desc.binamt.indexes] <- paste0('cust_fdesc_',gdbamt,'_ahourbin_amtaorder')
  # 1
  cust_fdesc_1hourbin_amtaorder <- order_observed_hourcut_1%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.binamt,amt_order)
  names(cust_fdesc_1hourbin_amtaorder)[desc.binamt.indexes] <- paste0('cust_fdesc_',gdbamt,'_1hourbin_amtaorder')
  # 2
  cust_fdesc_2hourbin_amtaorder <- order_observed_hourcut_2%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.binamt,amt_order)
  names(cust_fdesc_2hourbin_amtaorder)[desc.binamt.indexes] <- paste0('cust_fdesc_',gdbamt,'_2hourbin_amtaorder')
  # 3
  cust_fdesc_3hourbin_amtaorder <- order_observed_hourcut_3%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.binamt,amt_order)
  names(cust_fdesc_3hourbin_amtaorder)[desc.binamt.indexes] <- paste0('cust_fdesc_',gdbamt,'_3hourbin_amtaorder')
  # 4
  cust_fdesc_4hourbin_amtaorder <- order_observed_hourcut_4%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.binamt,amt_order)
  names(cust_fdesc_4hourbin_amtaorder)[desc.binamt.indexes] <- paste0('cust_fdesc_',gdbamt,'_4hourbin_amtaorder')
  # 5
  cust_fdesc_5hourbin_amtaorder <- order_observed_hourcut_5%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.binamt,amt_order)
  names(cust_fdesc_5hourbin_amtaorder)[desc.binamt.indexes] <- paste0('cust_fdesc_',gdbamt,'_5hourbin_amtaorder')
  # 6
  cust_fdesc_6hourbin_amtaorder <- order_observed_hourcut_6%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.binamt,amt_order)
  names(cust_fdesc_6hourbin_amtaorder)[desc.binamt.indexes] <- paste0('cust_fdesc_',gdbamt,'_6hourbin_amtaorder')
  # 7
  cust_fdesc_7hourbin_amtaorder <- order_observed_hourcut_7%>%
    group_by(appl_no,custorm_id)%>%summarise_each(desc.binamt,amt_order)
  names(cust_fdesc_7hourbin_amtaorder)[desc.binamt.indexes] <- paste0('cust_fdesc_',gdbamt,'_7hourbin_amtaorder')

  hourbinno <- c('a',1:7)
  text <- (paste0('hourbin_amtaorder_vargroup_list <- list(',paste0('cust_fdesc_',hourbinno,'hourbin_amtaorder',collapse = ','),')'))
  eval(parse(text = text))
  hourbin_amtaorder_vargroup <- Reduce(full_join,hourbin_amtaorder_vargroup_list)
  #######################################################################################################
  clnames <- names(hourbin_amtaorder_vargroup)
  vars_hourbin_input1 <- clnames[str_detect(clnames,'[1-7]hourbin')]
  vars_hourbin_input2 <- str_replace(vars_hourbin_input1,'[1-7]hourbin','ahourbin')
  varhbinput1 <- rep(vars_hourbin_input1,each=length(GDDIFF))
  varhbinput2 <- rep(vars_hourbin_input2,each=length(GDDIFF))
  hbfuncname <- rep(GDDIFF, times= length(vars_hourbin_input1),each = 1)

  # PMQM_GD_KV

  h_pmqm <- paste0(str_sub(str_extract(varhbinput1,'[1-7]hour'),1,1), str_sub(str_extract(varhbinput2,'ahour'),1,1))
  h_gd <- sapply(str_split(varhbinput1,pattern = '_'),function(x)x[3])
  h_kv <- sapply(str_split(varhbinput1,pattern = '_'),function(x)paste0(x[4],'_',x[5]))
  h_kv <- str_replace_all(h_kv,'[1-7]hourbin','hourbin') # 由于我们目前 只对订单状态求子集

  h_pmqm_gd_kv <- paste0(h_pmqm,'_',h_gd,'_',h_kv)
  h_fucname <- rep(GDDIFF,times=length(vars_hourbin_input1))
  outvarnames <- paste0('cust_fddif_',h_fucname,'_',h_pmqm_gd_kv)

  der_var_input <- cbind(outvarnames,h_fucname,varhbinput1,varhbinput2)
  der_hvar_logic <- paste0(der_var_input[,'outvarnames']," = ", der_var_input[,'h_fucname'],"(",der_var_input[,'varhbinput1'],",",der_var_input[,'varhbinput2'],")")
  text <- paste0("hourbin_amtaorder_vargroup01 <-hourbin_amtaorder_vargroup%>%mutate(",paste0(der_hvar_logic,collapse = ','),")")
  eval(parse(text=text))

  code.rename <- paste0("names(hourbin_amtaorder_vargroup01) <-  str_replace(names(hourbin_amtaorder_vargroup01),pattern = 'cust_',replacement = 'cust_",names(which(observe_interval_byday==obs_interval)),"_')",collapse = ";")
  eval(parse(text = code.rename))

  hourbin_amtaorder_vargroup01

}

#fdesc_hourbin_amtaorder(df_order_product%>%head(10),14)

#-----------------------------------------------------------#













################tsdesc_amtorderdiff.R###########################

tsdesc_amtorderdiff <- function(order_product,obs_interval)
{
  order_product_observed <- order_product%>%filter(difftime(appl_sbm_tm,time_order,units = "days")<=obs_interval)

  ################多次用到的数据集准备###################################################################################
  order_amt_time <- order_product_observed%>%select(appl_no,custorm_id,no_order,amt_order,time_order,order_time_int)%>%distinct()

  order_amt_time_diff <- order_amt_time%>%group_by(appl_no,custorm_id)%>%arrange(desc(time_order)) %>%
    mutate(lead_time=lead(order_time_int),lead_amt=lead(amt_order),timediff=order_time_int-lead_time,amtdiff=amt_order-lead_amt,vamt=amtdiff/timediff)%>%
    select(appl_no,custorm_id,timediff,amtdiff,vamt)

  cust_tsdesc_amtorderdiff  <- order_amt_time_diff%>%summarise_each(edesc.binamt,c(timediff,amtdiff,vamt) )
  namevec <- names(cust_tsdesc_amtorderdiff)
  len <- length(namevec)
  names(cust_tsdesc_amtorderdiff)[3:len] <- paste0('cust_tsdesc_amtorderdiff_',namevec[3:len])

  code.rename <- paste0("names(cust_tsdesc_amtorderdiff) <-  str_replace(names(cust_tsdesc_amtorderdiff),pattern = 'cust_',replacement = 'cust_",names(which(observe_interval_byday==obs_interval)),"_')",collapse = ";")
  eval(parse(text = code.rename))
  cust_tsdesc_amtorderdiff
}

#-------------------------------------------#









##############fdesc_hourbin_cntorder.R#############

data_preparation_time=function(df,time_gaps=14){
  df %>% filter(difftime(appl_sbm_tm,time_order,units = "days")<=time_gaps)
}

n_hour2305_cntorder=function(time_order){
  flag=(hour(time_order)==23 | (hour(time_order)>=0 & hour(time_order)<5))
  n=sum(if_else(flag,1,0))
  return(n)
}
exist_hour2305_cntorder=function(time_order){
  flag=(hour(time_order)==23 | (hour(time_order)>=0 & hour(time_order)<5))
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

n_hour0509_cntorder=function(time_order){
  flag=(hour(time_order)>=5 & hour(time_order)<9)
  n=sum(if_else(flag,1,0))
  return(n)
}
exist_hour0509_cntorder=function(time_order){
  flag=(hour(time_order)>=5 & hour(time_order)<9)
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

n_hour0912_cntorder=function(time_order){
  flag=(hour(time_order)>=9 & hour(time_order)<12)
  n=sum(if_else(flag,1,0))
  return(n)
}
exist_hour0912_cntorder=function(time_order){
  flag=(hour(time_order)>=9 & hour(time_order)<12)
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

n_hour1214_cntorder=function(time_order){
  flag=(hour(time_order)>=12 & hour(time_order)<14)
  n=sum(if_else(flag,1,0))
  return(n)
}
exist_hour1214_cntorder=function(time_order){
  flag=(hour(time_order)>=12 & hour(time_order)<14)
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

n_hour1418_cntorder=function(time_order){
  flag=(hour(time_order)>=14 & hour(time_order)<18)
  n=sum(if_else(flag,1,0))
  return(n)
}
exist_hour1418_cntorder=function(time_order){
  flag=(hour(time_order)>=14 & hour(time_order)<18)
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

n_hour1820_cntorder=function(time_order){
  flag=(hour(time_order)>=18 & hour(time_order)<20)
  n=sum(if_else(flag,1,0))
  return(n)
}
exist_hour1820_cntorder=function(time_order){
  flag=(hour(time_order)>=18 & hour(time_order)<20)
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}


n_hour2023_cntorder=function(time_order){
  flag=(hour(time_order)>=20 & hour(time_order)<23)
  n=sum(if_else(flag,1,0))
  return(n)
}
exist_hour2023_cntorder=function(time_order){
  flag=(hour(time_order)>=20 & hour(time_order)<23)
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

# library(dplyr)
# library(entropy)
# library(lubridate)
# library(stringr)
# library(parallel)
#
# library(tidyr)
#library(dplyr)
desc.hour_order_cnt=funs(n_hour2305_cntorder,n_hour0509_cntorder,n_hour0912_cntorder,
                         n_hour1214_cntorder,n_hour1418_cntorder,n_hour1820_cntorder,
                         n_hour2023_cntorder)

desc.hour_order_exist=funs(exist_hour2305_cntorder,exist_hour0509_cntorder,exist_hour0912_cntorder,
                           exist_hour1214_cntorder,exist_hour1418_cntorder,exist_hour1820_cntorder,
                           exist_hour2023_cntorder)

desc.hourcount <- funs(sum(.,na.rm = T),mean(.,na.rm = T),min(.,na.rm = T),max(.,na.rm = T),
                       median(.,na.rm = T),sd(.,na.rm = T),var(.,na.rm = T),
                       cv,range,skew,kurto,qu,ql,qd,entropy,normentropy)
gdhourcnt <- names(desc.hourcount)

#计算逻辑
compute_var_hour=function(df,time_names='2w'){
  #1.计算变量
  #第一部分：计算每个时间段的订单量
  a=df %>% select(appl_no,custorm_id,no_order,time_order) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hour_order_cnt,time_order)

  #第二部分：计算每个时间段是否有订单
  b=df %>% select(appl_no,custorm_id,no_order,time_order) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hour_order_exist,time_order)

  #第三部分：计算7个时间段订单量后面的16个统计量
  c=a %>%
    gather(hour_bin,hour_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,hour_order_cnt)
  names(c)[3:length(c)] <- paste0(gdhourcnt,'_hour_cntorder')

  #2.合并变量框
  var_list=list(a,b,c)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])

  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('_n_')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(desc.hour_order_cnt),'n_hour',''),'_cntorder',''),
                 'all','_n_hour_cntorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))

  #差着一个判断两周是否有订单的变量
  var=var %>%mutate(cust_fdesc_exist_hour_cntorder=if_else(cust_fdesc_sum_hour_cntorder>0,1,0))
  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)
}

#测试
#df=df_order_product%>%filter(appl_no=='20160813110005997260') %>%
# data_preparation_time() %>% compute_var_hour()


fdesc_hourbin_cntorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_hour(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
#fdesc_hourbin_cntorder(time_gap=30)

#-------------------------------------------------------#


#############fdesc_sts_cntorder.R###################
n_stsfail_cntorder=function(sts_order){
  flag=(sts_order %in% c('已取消', '订单取消' ,'已取消订单','配送退货' ))
  n=sum(if_else(flag,1,0))
  return(n)
}

n_stssuccess_cntorder=function(sts_order){
  flag=(sts_order %in% c('完成','充值成功'))
  n=sum(if_else(flag,1,0))
  return(n)
}

exist_stsfail_cntorder=function(sts_order){
  flag=(sts_order %in% c('已取消', '订单取消' ,'已取消订单','配送退货' ))
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

exist_stssuccess_cntorder=function(sts_order){
  flag=(sts_order %in% c('完成','充值成功'))
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

desc.sts_cnt=funs(n_stsfail_cntorder,n_stssuccess_cntorder)
desc.sts_exist=funs(exist_stsfail_cntorder,exist_stssuccess_cntorder)


#计算逻辑
compute_var_sts=function(df,time_names='_2w'){

  #2.计算变量
  #第一部分：计算不同状态的订单量
  a=df %>% select(appl_no,custorm_id,no_order,sts_order) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.sts_cnt,sts_order)

  #第二部分：计算每个状态是否有订单

  b=df %>% select(appl_no,custorm_id,no_order,sts_order) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.sts_exist,sts_order)

  #第三部分：计算汇总的订单情况

  c=a %>%
    gather(sts_bin,sts_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise(sum_sts_cntorder=sum(sts_order_cnt) )


  #2.合并变量框
  var_list=list(a,b,c)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])

  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('_n_')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(desc.sts_cnt),'n_sts',''),'_cntorder',''),
                 'all','_n_sts_cntorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))

  #差着一个判断两周是否有订单的变量
  var=var %>%mutate(cust_fdesc_exist_sts_cntorder=if_else(cust_fdesc_sum_sts_cntorder>0,1,0))
  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)

}

fdesc_sts_cntorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_sts(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
#  fdesc_sts_cntorder(time_gap=Inf)

#----------------------------------------------------------------#





################################fdesc_invoicecontent_cntorder.R##################

n_invoicecontentdetail_cntorder=function(invoice_content){
  sum(if_else(!is.na(str_match(invoice_content,'明细')),1,0))
}
exist_invoicecontentdetail_cntorder=function(invoice_content){
  n=sum(if_else(!is.na(str_match(invoice_content,'明细')),1,0))
  return(if_else(n>0,1,0))
}

n_invoicecontentoffice_cntorder=function(invoice_content){
  sum(if_else(!is.na(str_match(invoice_content,'办公用品')),1,0))
}
exist_invoicecontentoffice_cntorder=function(invoice_content){
  n=sum(if_else(!is.na(str_match(invoice_content,'办公用品')),1,0))
  return(if_else(n>0,1,0))
}

n_invoicecontentcomputer_cntorder=function(invoice_content){
  sum(if_else(!is.na(str_match(invoice_content,'电脑配件')),1,0))
}
exist_invoicecontentcomputer_cntorder=function(invoice_content){
  n=sum(if_else(!is.na(str_match(invoice_content,'电脑配件')),1,0))
  return(if_else(n>0,1,0))
}

n_invoicecontentsupplies_cntorder=function(invoice_content){
  sum(if_else(!is.na(str_match(invoice_content,'耗材')),1,0))
}
exist_invoicecontentsupplies_cntorder=function(invoice_content){
  n=sum(if_else(!is.na(str_match(invoice_content,'耗材')),1,0))
  return(if_else(n>0,1,0))
}

n_invoicecontentbooks_cntorder=function(invoice_content){
  flag_1=!is.na(str_match(invoice_content,'图书'))
  flag_2=!is.na(str_match(invoice_content,'资料'))
  flag_3=!is.na(str_match(invoice_content,'教材'))
  n=sum(if_else((flag_1 | flag_2 | flag_3),1,0))
  return(n)
}
exist_invoicecontentbooks_cntorder=function(invoice_content){
  flag_1=!is.na(str_match(invoice_content,'图书'))
  flag_2=!is.na(str_match(invoice_content,'资料'))
  flag_3=!is.na(str_match(invoice_content,'教材'))
  n=sum(if_else((flag_1 | flag_2 | flag_3),1,0))
  return(if_else(n>0,1,0))
}

desc.invoicecontent_cnt=funs(n_invoicecontentdetail_cntorder,n_invoicecontentoffice_cntorder,
                             n_invoicecontentcomputer_cntorder,n_invoicecontentsupplies_cntorder,
                             n_invoicecontentbooks_cntorder)

desc.invoicecontent_exist=funs(exist_invoicecontentdetail_cntorder,exist_invoicecontentoffice_cntorder,
                               exist_invoicecontentcomputer_cntorder,exist_invoicecontentsupplies_cntorder,
                               exist_invoicecontentbooks_cntorder)

compute_var_invoicecontent=function(df,time_names='_2w'){
  #2.计算变量
  #第一部分：计算发票内容的订单量
  a=df %>% select(appl_no,custorm_id,no_order,invoice_content) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.invoicecontent_cnt,invoice_content)

  #第二部分：计算不同发票内容是否有订单

  b=df %>% select(appl_no,custorm_id,no_order,invoice_content) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.invoicecontent_exist,invoice_content)

  #第三部分：计算汇总的订单情况

  c=a %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,paytype_order_cnt)
  names(c)[3:length(c)] <- paste0(gdhourcnt,'_invoicecontent_cntorder')

  #2.合并变量框
  var_list=list(a,b,c)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])

  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('_n_')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(desc.invoicecontent_cnt),'n_invoicecontent',''),'_cntorder',''),
                 'all','_n_invoicecontent_cntorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))

  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)

}

fdesc_invoicecontent_cntorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_invoicecontent(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
#  fdesc_invoicecontent_cntorder(time_gap=360)


#------------------------------------------------------------------------#




#############################fdesc_invoicehead_cntorder.R##################

n_invoiceheadindividual_cntorder=function(invoice_head){
  individual="个人"
  n=sum(if_else(is.na(str_match(invoice_head,individual)),0,1))
  return(n)
}

n_invoiceheadcompany_cntorder=function(invoice_head){
  company="公司"
  n=sum(if_else(is.na(str_match(invoice_head,company)),0,1))
  return(n)
}
desc.invoicehead_cnt=funs(n_invoiceheadindividual_cntorder,n_invoiceheadcompany_cntorder)

exist_invoiceheadindividual_cntorder=function(invoice_head){
  individual="个人"
  n=sum(if_else(is.na(str_match(invoice_head,individual)),0,1))
  return(if_else(n>0,1,0))
}

exist_invoiceheadcompany_cntorder=function(invoice_head){
  company="公司"
  n=sum(if_else(is.na(str_match(invoice_head,company)),0,1))
  return(if_else(n>0,1,0))
}

desc.invoicehead_exist=funs(exist_invoiceheadindividual_cntorder,exist_invoiceheadcompany_cntorder)


compute_var_invoicehead=function(df,time_names='_2w'){
  #2.计算变量
  #第一部分：计算不同状态的订单量
  a=df %>% select(appl_no,custorm_id,no_order,invoice_head) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.invoicehead_cnt,invoice_head)

  #第二部分：计算每个状态是否有订单

  b=df %>% select(appl_no,custorm_id,no_order,invoice_head) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.invoicehead_exist,invoice_head)

  #第三部分：计算汇总的订单情况

  c=a %>%
    gather(sts_bin,sts_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise(sum_invoicehead_cntorder=sum(sts_order_cnt) )

  #2.合并变量框
  var_list=list(a,b,c)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])
  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('_n_')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(desc.invoicehead_cnt),'n_invoicehead',''),'_cntorder',''),
                 'all','_n_invoicehead_cntorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))


  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)

}

fdesc_invoicehead_cntorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_invoicehead(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
# fdesc_invoicehead_cntorder(time_gap=360)

#------------------------------------------------------------#








#######################################fdesc_accname_cntorder.R##################

n_accnamephone_cntorder=function(name_acc){
  phone='[1][0-9]{10}'
  n=sum(if_else(is.na(str_match(name_acc,phone)),0,1))
  return(n)
}

n_accnameemail_cntorder=function(name_acc){
  email = "[\\w-\\.]+@[\\w]+\\.+[a-z]{2,3}"
  n=sum(if_else(is.na(str_match(name_acc,email)),0,1))
  return(n)
}


n_accnamelogname_cntorder=function(name_acc){
  logname="[a-zA-z]+"
  n=sum(if_else(is.na(str_match(name_acc,logname)),0,1))
  return(n)
}


n_accnamelognameNumb_cntorder=function(name_acc){
  lognameNumb="[a-zA-z_]+[0-9]+"
  n=sum(if_else(is.na(str_match(name_acc,lognameNumb)),0,1))
  return(n)
}

n_accnamechinese_cntorder=function(name_acc){
  chinese="[\u4e00-\u9fa5]+"
  n=sum(if_else(is.na(str_match(name_acc,chinese)),0,1))
  return(n)
}

n_accnamechineseNumb_cntorder=function(name_acc){
  chineseNumb="[\u4e00-\u9fa5]+[\\w]+"
  n=sum(if_else(is.na(str_match(name_acc,chineseNumb)),0,1))
  return(n)
}

n_accnamejdname_cntorder=function(name_acc){
  jdname="jd_[\\w]+"
  n=sum(if_else(is.na(str_match(name_acc,jdname)),0,1))
  return(n)
}

desc.accname_cnt=funs(n_accnamephone_cntorder,n_accnameemail_cntorder,n_accnamelogname_cntorder,
                      n_accnamelognameNumb_cntorder,n_accnamechinese_cntorder,n_accnamechineseNumb_cntorder,
                      n_accnamejdname_cntorder)


exist_accnamephone_cntorder=function(name_acc){
  phone='[1][0-9]{10}'
  n=sum(if_else(is.na(str_match(name_acc,phone)),0,1))
  return(if_else(n>0,1,0))
}

exist_accnameemail_cntorder=function(name_acc){
  email = "[\\w-\\.]+@[\\w]+\\.+[a-z]{2,3}"
  n=sum(if_else(is.na(str_match(name_acc,email)),0,1))
  return(if_else(n>0,1,0))
}


exist_accnamelogname_cntorder=function(name_acc){
  logname="[a-zA-z]+"
  n=sum(if_else(is.na(str_match(name_acc,logname)),0,1))
  return(if_else(n>0,1,0))
}


exist_accnamelognameNumb_cntorder=function(name_acc){
  lognameNumb="[a-zA-z_]+[0-9]+"
  n=sum(if_else(is.na(str_match(name_acc,lognameNumb)),0,1))
  return(if_else(n>0,1,0))
}

exist_accnamechinese_cntorder=function(name_acc){
  chinese="[\u4e00-\u9fa5]+"
  n=sum(if_else(is.na(str_match(name_acc,chinese)),0,1))
  return(if_else(n>0,1,0))
}

exist_accnamechineseNumb_cntorder=function(name_acc){
  chineseNumb="[\u4e00-\u9fa5]+[\\w]+"
  n=sum(if_else(is.na(str_match(name_acc,chineseNumb)),0,1))
  return(if_else(n>0,1,0))
}

exist_accnamejdname_cntorder=function(name_acc){
  jdname="jd_[\\w]+"
  n=sum(if_else(is.na(str_match(name_acc,jdname)),0,1))
  return(if_else(n>0,1,0))
}

desc.accname_exist=funs(exist_accnamephone_cntorder,exist_accnameemail_cntorder,exist_accnamelogname_cntorder,
                        exist_accnamelognameNumb_cntorder,exist_accnamechinese_cntorder,exist_accnamechineseNumb_cntorder,
                        exist_accnamejdname_cntorder)





compute_var_accname=function(df,time_names='_2w'){
  #2.计算变量
  #第一部分：计算不同账户名的订单量
  a=df %>% select(appl_no,custorm_id,no_order,name_acc) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.accname_cnt,name_acc)

  #第二部分：计算不同账户名是否有订单

  b=df %>% select(appl_no,custorm_id,no_order,name_acc) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.accname_exist,name_acc)

  #第三部分：计算汇总的订单情况

  c=a %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,paytype_order_cnt)
  names(c)[3:length(c)] <- paste0(gdhourcnt,'_accname_cntorder')

  #2.合并变量框
  var_list=list(a,b,c)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])

  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('_n_')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(desc.accname_cnt),'n_accname',''),'_cntorder',''),
                 'all','_n_accname_cntorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))

  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)

}

fdesc_accname_cntorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_accname(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
# fdesc_accname_cntorder(time_gap=360)

#---------------------------------------------------------------------------#








###############################################

n_payinstalments_cntorder=function(type_pay){
  flag=(type_pay %in% c('分期付款','分期付款(招行)'))
  n=sum(if_else(flag,1,0))
  return(n)
}


n_payonlinepayment_cntorder=function(type_pay){
  flag=(type_pay =='在线支付')
  n=sum(if_else(flag,1,0))
  return(n)
}

n_paycashondelivery_cntorder=function(type_pay){
  flag=(type_pay =='货到付款')
  n=sum(if_else(flag,1,0))
  return(n)
}

n_paypickedup_cntorder=function(type_pay){
  flag=(type_pay =='上门自提')
  n=sum(if_else(flag,1,0))
  return(n)
}

n_paypostoffice_cntorder=function(type_pay){
  flag=(type_pay =='邮局汇款')
  n=sum(if_else(flag,1,0))
  return(n)
}

n_paycompanytransfer_cntorder=function(type_pay){
  flag=(type_pay =='公司转账')
  n=sum(if_else(flag,1,0))
  return(n)
}

n_payioustopay_cntorder=function(type_pay){
  flag=(type_pay =='白条支付')
  n=sum(if_else(flag,1,0))
  return(n)
}

n_payagencypayment_cntorder=function(type_pay){
  flag=(type_pay =='高校代理-代理支付')
  n=sum(if_else(flag,1,0))
  return(n)
}

n_paynull_cntorder=function(type_pay){
  flag=(is.na(type_pay))
  n=sum(if_else(flag,1,0))
  return(n)
}
desc.paytype_cnt=funs(n_payinstalments_cntorder,n_payonlinepayment_cntorder,
                      n_paycashondelivery_cntorder,n_paypickedup_cntorder,
                      n_paypostoffice_cntorder,n_paycompanytransfer_cntorder,
                      n_payioustopay_cntorder,n_payagencypayment_cntorder,n_paynull_cntorder)

#########################################fdesc_pay_cntorder.R###################
exist_payinstalments_cntorder=function(type_pay){
  flag=(type_pay %in% c('分期付款','分期付款(招行)'))
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

exist_payonlinepayment_cntorder=function(type_pay){
  flag=(type_pay =='在线支付')
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

exist_paycashondelivery_cntorder=function(type_pay){
  flag=(type_pay =='货到付款')
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

exist_paypickedup_cntorder=function(type_pay){
  flag=(type_pay =='上门自提')
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

exist_paypostoffice_cntorder=function(type_pay){
  flag=(type_pay =='邮局汇款')
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

exist_paycompanytransfer_cntorder=function(type_pay){
  flag=(type_pay =='公司转账')
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

exist_payioustopay_cntorder=function(type_pay){
  flag=(type_pay =='白条支付')
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

exist_payagencypayment_cntorder=function(type_pay){
  flag=(type_pay =='高校代理-代理支付')
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

exist_paynull_cntorder=function(type_pay){
  flag=(is.na(type_pay))
  n=sum(if_else(flag,1,0))
  return(if_else(n>0,1,0))
}

desc.paytype_exist=funs(exist_payinstalments_cntorder,exist_payonlinepayment_cntorder,
                        exist_paycashondelivery_cntorder,exist_paypickedup_cntorder,
                        exist_paypostoffice_cntorder,exist_paycompanytransfer_cntorder,
                        exist_payioustopay_cntorder,exist_payagencypayment_cntorder,exist_paynull_cntorder)


#计算逻辑
compute_var_paytype=function(df,time_names='_2w'){

  #2.计算变量
  #第一部分：计算不同支付状态的订单量
  a=df %>% select(appl_no,custorm_id,no_order,type_pay) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.paytype_cnt,type_pay)

  #第二部分：计算每个状态是否有订单

  b=df %>% select(appl_no,custorm_id,no_order,type_pay) %>% unique() %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.paytype_exist,type_pay)

  #第三部分：计算汇总的订单情况

  c=a %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,paytype_order_cnt)
  names(c)[3:length(c)] <- paste0(gdhourcnt,'_pay_cntorder')

  #2.合并变量框
  var_list=list(a,b,c)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])

  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('_n_')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(desc.paytype_cnt),'n_pay',''),'_cntorder',''),
                 'all','_n_pay_cntorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))

  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)

}

fdesc_pay_cntorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_paytype(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
# fdesc_pay_cntorder(time_gap=30)


#-----------------------------------------------------#







##################################fdesc_accname_amtorder.R#####################

sum_accnamephone_amtorder=function(name_acc,amt_order){
  phone='[1][0-9]{10}'
  n=sum(if_else(is.na(str_match(name_acc,phone)),0,amt_order))
  return(n)
}

sum_accnameemail_amtorder=function(name_acc,amt_order){
  email = "[\\w-\\.]+@[\\w]+\\.+[a-z]{2,3}"
  n=sum(if_else(is.na(str_match(name_acc,email)),0,amt_order))
  return(n)
}


sum_accnamelogname_amtorder=function(name_acc,amt_order){
  logname="[a-zA-z]+"
  n=sum(if_else(is.na(str_match(name_acc,logname)),0,amt_order))
  return(n)
}


sum_accnamelognamenumb_amtorder=function(name_acc,amt_order){
  lognameNumb="[a-zA-z_]+[0-9]+"
  n=sum(if_else(is.na(str_match(name_acc,lognameNumb)),0,amt_order))
  return(n)
}

sum_accnamechinese_amtorder=function(name_acc,amt_order){
  chinese="[\u4e00-\u9fa5]+"
  n=sum(if_else(is.na(str_match(name_acc,chinese)),0,amt_order))
  return(n)
}

sum_accnamechinesenumb_amtorder=function(name_acc,amt_order){
  chineseNumb="[\u4e00-\u9fa5]+[\\w]+"
  n=sum(if_else(is.na(str_match(name_acc,chineseNumb)),0,amt_order))
  return(n)
}

sum_accnamejdname_amtorder=function(name_acc,amt_order){
  jdname="jd_[\\w]+"
  n=sum(if_else(is.na(str_match(name_acc,jdname)),0,amt_order))
  return(n)
}



mean_accnamephone_amtorder=function(name_acc,amt_order){
  phone='[1][0-9]{10}'
  n=sum(if_else(is.na(str_match(name_acc,phone)),0,amt_order))
  return(n)
}

mean_accnameemail_amtorder=function(name_acc,amt_order){
  email = "[\\w-\\.]+@[\\w]+\\.+[a-z]{2,3}"
  n=sum(if_else(is.na(str_match(name_acc,email)),0,amt_order))
  return(n)
}


mean_accnamelogname_amtorder=function(name_acc,amt_order){
  logname="[a-zA-z]+"
  n=sum(if_else(is.na(str_match(name_acc,logname)),0,amt_order))
  return(n)
}


mean_accnamelognamenumb_amtorder=function(name_acc,amt_order){
  lognameNumb="[a-zA-z_]+[0-9]+"
  n=sum(if_else(is.na(str_match(name_acc,lognameNumb)),0,amt_order))
  return(n)
}

mean_accnamechinese_amtorder=function(name_acc,amt_order){
  chinese="[\u4e00-\u9fa5]+"
  n=sum(if_else(is.na(str_match(name_acc,chinese)),0,amt_order))
  return(n)
}

mean_accnamechinesenumb_amtorder=function(name_acc,amt_order){
  chineseNumb="[\u4e00-\u9fa5]+[\\w]+"
  n=sum(if_else(is.na(str_match(name_acc,chineseNumb)),0,amt_order))
  return(n)
}

mean_accnamejdname_amtorder=function(name_acc,amt_order){
  jdname="jd_[\\w]+"
  n=sum(if_else(is.na(str_match(name_acc,jdname)),0,amt_order))
  return(n)
}






#计算逻辑
compute_var_accname_amt=function(df,time_names='_2w'){

  #2.计算变量
  #第一部分：计算不同支付状态的订单金额的总和
  a=df %>% select(appl_no,custorm_id,name_acc,amt_order,no_order) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise(sum_accnamephone_amtorder=sum_accnamephone_amtorder(name_acc,amt_order),
              sum_accnameemail_amtorder=sum_accnameemail_amtorder(name_acc,amt_order),
              sum_accnamelogname_amtorder=sum_accnamelogname_amtorder(name_acc,amt_order),
              sum_accnamelognamenumb_amtorder=sum_accnamelognamenumb_amtorder(name_acc,amt_order),
              sum_accnamechinese_amtorder=sum_accnamechinese_amtorder(name_acc,amt_order),
              sum_accnamechinesenumb_amtorder=sum_accnamechinesenumb_amtorder(name_acc,amt_order),
              sum_accnamejdname_amtorder=sum_accnamejdname_amtorder(name_acc,amt_order)	)

  #第二部分：计算不同支付状态的订单金额的均值

  b=df %>% select(appl_no,custorm_id,name_acc,amt_order,no_order) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise(mean_accnamephone_amtorder=mean_accnamephone_amtorder(name_acc,amt_order),
              mean_accnameemail_amtorder=mean_accnameemail_amtorder(name_acc,amt_order),
              mean_accnamelogname_amtorder=mean_accnamelogname_amtorder(name_acc,amt_order),
              mean_accnamelognamenumb_amtorder=mean_accnamelognamenumb_amtorder(name_acc,amt_order),
              mean_accnamechinese_amtorder=mean_accnamechinese_amtorder(name_acc,amt_order),
              mean_accnamechinesenumb_amtorder=mean_accnamechinesenumb_amtorder(name_acc,amt_order),
              mean_accnamejdname_amtorder=mean_accnamejdname_amtorder(name_acc,amt_order)	)

  #第三部分：计算汇总的订单情况

  c=a %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,paytype_order_cnt)
  names(c)[3:length(c)] <- paste0(gdhourcnt,'_sumaccname_amtorder')

  d=b %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,paytype_order_cnt)
  names(d)[3:length(d)] <- paste0(gdhourcnt,'_meanaccname_amtorder')

  #2.合并变量框
  var_list=list(a,b,c,d)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])

  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('sum_accname')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum_sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(a),'sum_accname',''),'_amtorder',''),
                 'all','_sum_accname_amtorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))

  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)

}

fdesc_accname_amtorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_accname_amt(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
# fdesc_accname_amtorder(time_gap=180)

#---------------------------------------------------------------------------#






#############################fdesc_pay_amtorder.R####################

sum_payinstalments_amtorder=function(type_pay,amt_order){
  flag=(type_pay %in% c('分期付款','分期付款(招行)'))
  n=sum(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}


sum_payonlinepayment_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='在线支付')
  n=sum(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

sum_paycashondelivery_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='货到付款')
  n=sum(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

sum_paypickedup_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='上门自提')
  n=sum(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

sum_paypostoffice_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='邮局汇款')
  n=sum(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

sum_paycompanytransfer_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='公司转账')
  n=sum(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

sum_payioustopay_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='白条支付')
  n=sum(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

sum_payagencypayment_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='高校代理-代理支付')
  n=sum(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

sum_paynull_amtorder=function(type_pay,amt_order){
  flag=(is.na(type_pay))
  n=sum(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}




mean_payinstalments_amtorder=function(type_pay,amt_order){
  flag=(type_pay %in% c('分期付款','分期付款(招行)'))
  n=mean(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}


mean_payonlinepayment_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='在线支付')
  n=mean(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

mean_paycashondelivery_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='货到付款')
  n=mean(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

mean_paypickedup_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='上门自提')
  n=mean(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

mean_paypostoffice_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='邮局汇款')
  n=mean(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

mean_paycompanytransfer_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='公司转账')
  n=mean(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

mean_payioustopay_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='白条支付')
  n=mean(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

mean_payagencypayment_amtorder=function(type_pay,amt_order){
  flag=(type_pay =='高校代理-代理支付')
  n=mean(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}

mean_paynull_amtorder=function(type_pay,amt_order){
  flag=(is.na(type_pay))
  n=mean(if_else(flag,amt_order,0),na.rm=T)
  return(n)
}


#计算逻辑
compute_var_paytype_amt=function(df,time_names='_2w'){

  #2.计算变量
  #第一部分：计算不同支付状态的订单金额的总和
  a=df %>% select(appl_no,custorm_id,type_pay,amt_order,no_order) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise(sum_payinstalments_amtorder=sum_payinstalments_amtorder(type_pay,amt_order),
              sum_payonlinepayment_amtorder=sum_payonlinepayment_amtorder(type_pay,amt_order),
              sum_paycashondelivery_amtorder=sum_paycashondelivery_amtorder(type_pay,amt_order),
              sum_paypickedup_amtorder=sum_paypickedup_amtorder(type_pay,amt_order),
              sum_paypostoffice_amtorder=sum_paypostoffice_amtorder(type_pay,amt_order),
              sum_paycompanytransfer_amtorder=sum_paycompanytransfer_amtorder(type_pay,amt_order),
              sum_payioustopay_amtorder=sum_payioustopay_amtorder(type_pay,amt_order),
              sum_payagencypayment_amtorder=sum_payagencypayment_amtorder(type_pay,amt_order),
              sum_paynull_amtorder=sum_paynull_amtorder(type_pay,amt_order))

  #第二部分：计算不同支付状态的订单金额的均值

  b=df %>% select(appl_no,custorm_id,type_pay,amt_order,no_order) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise(mean_payinstalments_amtorder=mean_payinstalments_amtorder(type_pay,amt_order),
              mean_payonlinepayment_amtorder=mean_payonlinepayment_amtorder(type_pay,amt_order),
              mean_paycashondelivery_amtorder=mean_paycashondelivery_amtorder(type_pay,amt_order),
              mean_paypickedup_amtorder=mean_paypickedup_amtorder(type_pay,amt_order),
              mean_paypostoffice_amtorder=mean_paypostoffice_amtorder(type_pay,amt_order),
              mean_paycompanytransfer_amtorder=mean_paycompanytransfer_amtorder(type_pay,amt_order),
              mean_payioustopay_amtorder=mean_payioustopay_amtorder(type_pay,amt_order),
              mean_payagencypayment_amtorder=mean_payagencypayment_amtorder(type_pay,amt_order),
              mean_paynull_amtorder=mean_paynull_amtorder(type_pay,amt_order))

  #第三部分：计算汇总的订单情况

  c=a %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,paytype_order_cnt)
  names(c)[3:length(c)] <- paste0(gdhourcnt,'_sumpay_amtorder')

  d=b %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,paytype_order_cnt)
  names(d)[3:length(d)] <- paste0(gdhourcnt,'_meanpay_amtorder')

  #2.合并变量框
  var_list=list(a,b,c,d)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])

  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('sum_pay')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum_sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(a)[3:length(a)],'sum_pay',''),'_amtorder',''),
                 'all','_sum_pay_amtorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))

  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)
}

fdesc_pay_amtorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_paytype_amt(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
# fdesc_pay_amtorder(time_gap=180)

#----------------------------------------------------------------#

















##############################################fdesc_invoicehead_amtorder.R##################
sum_invoiceheadindividual_amtorder=function(invoice_head,amt_order){
  individual="个人"
  n=sum(if_else(is.na(str_match(invoice_head,individual)),0,amt_order))
  return(n)
}

sum_invoiceheadcompany_amtorder=function(invoice_head,amt_order){
  company="公司"
  n=sum(if_else(is.na(str_match(invoice_head,company)),0,amt_order))
  return(n)
}


mean_invoiceheadindividual_amtorder=function(invoice_head,amt_order){
  individual="个人"
  n=mean(if_else(is.na(str_match(invoice_head,individual)),0,amt_order))
  return(n)
}

mean_invoiceheadcompany_amtorder=function(invoice_head,amt_order){
  company="公司"
  n=mean(if_else(is.na(str_match(invoice_head,company)),0,amt_order))
  return(n)
}





#计算逻辑
compute_var_invoicehead_amt=function(df,time_names='_2w'){

  #2.计算变量
  #第一部分：计算不同发票抬头的订单金额的总和
  a=df %>% select(appl_no,custorm_id,invoice_head,amt_order,no_order) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise(sum_invoiceheadindividual_amtorder=sum_invoiceheadindividual_amtorder(invoice_head,amt_order),
              sum_invoiceheadcompany_amtorder=sum_invoiceheadcompany_amtorder(invoice_head,amt_order)	)

  #第二部分：计算不同发票抬头的订单金额的均值

  b=df %>% select(appl_no,custorm_id,invoice_head,amt_order,no_order) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise(mean_invoiceheadindividual_amtorder=mean_invoiceheadindividual_amtorder(invoice_head,amt_order),
              mean_invoiceheadcompany_amtorder=mean_invoiceheadcompany_amtorder(invoice_head,amt_order))

  c=a %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise(sum_suminvoicehead_amtorder=sum(paytype_order_cnt))


  #2.合并变量框
  var_list=list(a,b,c)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])

  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('sum_invoicehead')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum_sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(a)[3:length(a)],'sum_invoicehead',''),'_amtorder',''),
                 'all','_sum_invoicehead_amtorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))

  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)

}

fdesc_invoicehead_amtorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_invoicehead_amt(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
# fdesc_invoicehead_amtorder(time_gap=180)


#-----------------------------------------------------------#













####################################fdesc_invoicecontent_amtorder.R################
sum_invoicecontentdetail_amtorder=function(invoice_content,amt_order){
  sum(if_else(!is.na(str_match(invoice_content,'明细')),amt_order,0))
}


sum_invoicecontentoffice_amtorder=function(invoice_content,amt_order){
  sum(if_else(!is.na(str_match(invoice_content,'办公用品')),amt_order,0))
}


sum_invoicecontentcomputer_amtorder=function(invoice_content,amt_order){
  sum(if_else(!is.na(str_match(invoice_content,'电脑配件')),amt_order,0))
}


sum_invoicecontentsupplies_amtorder=function(invoice_content,amt_order){
  sum(if_else(!is.na(str_match(invoice_content,'耗材')),amt_order,0))
}


sum_invoicecontentbooks_amtorder=function(invoice_content,amt_order){
  flag_1=!is.na(str_match(invoice_content,'图书'))
  flag_2=!is.na(str_match(invoice_content,'资料'))
  flag_3=!is.na(str_match(invoice_content,'教材'))
  n=sum(if_else((flag_1 | flag_2 | flag_3),amt_order,0))
  return(n)
}





mean_invoicecontentdetail_amtorder=function(invoice_content,amt_order){
  mean(if_else(!is.na(str_match(invoice_content,'明细')),amt_order,0))
}


mean_invoicecontentoffice_amtorder=function(invoice_content,amt_order){
  mean(if_else(!is.na(str_match(invoice_content,'办公用品')),amt_order,0))
}


mean_invoicecontentcomputer_amtorder=function(invoice_content,amt_order){
  mean(if_else(!is.na(str_match(invoice_content,'电脑配件')),amt_order,0))
}


mean_invoicecontentsupplies_amtorder=function(invoice_content,amt_order){
  mean(if_else(!is.na(str_match(invoice_content,'耗材')),amt_order,0))
}


mean_invoicecontentbooks_amtorder=function(invoice_content,amt_order){
  flag_1=!is.na(str_match(invoice_content,'图书'))
  flag_2=!is.na(str_match(invoice_content,'资料'))
  flag_3=!is.na(str_match(invoice_content,'教材'))
  n=mean(if_else((flag_1 | flag_2 | flag_3),amt_order,0))
  return(n)
}




#计算逻辑
compute_var_invoicecontent_amt=function(df,time_names='_2w'){

  #2.计算变量
  #第一部分：计算不同发票内容的订单金额的总和
  a=df %>% select(appl_no,custorm_id,invoice_content,amt_order,no_order) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise(sum_invoicecontentdetail_amtorder=sum_invoicecontentdetail_amtorder(invoice_content,amt_order),
              sum_invoicecontentoffice_amtorder=sum_invoicecontentoffice_amtorder(invoice_content,amt_order),
              sum_invoicecontentcomputer_amtorder=sum_invoicecontentcomputer_amtorder(invoice_content,amt_order),
              sum_invoicecontentsupplies_amtorder=sum_invoicecontentsupplies_amtorder(invoice_content,amt_order),
              sum_invoicecontentbooks_amtorder=sum_invoicecontentbooks_amtorder(invoice_content,amt_order))

  #第二部分：计算不同发票内容的订单金额的均值

  b=df %>% select(appl_no,custorm_id,invoice_content,amt_order,no_order) %>% distinct() %>%
    group_by(appl_no,custorm_id) %>%
    summarise(mean_invoicecontentdetail_amtorder=mean_invoicecontentdetail_amtorder(invoice_content,amt_order),
              mean_invoicecontentoffice_amtorder=mean_invoicecontentoffice_amtorder(invoice_content,amt_order),
              mean_invoicecontentcomputer_amtorder=mean_invoicecontentcomputer_amtorder(invoice_content,amt_order),
              mean_invoicecontentsupplies_amtorder=mean_invoicecontentsupplies_amtorder(invoice_content,amt_order),
              mean_invoicecontentbooks_amtorder=mean_invoicecontentbooks_amtorder(invoice_content,amt_order))

  #第三部分：计算汇总的订单情况

  c=a %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,paytype_order_cnt)
  names(c)[3:length(c)] <- paste0(gdhourcnt,'_suminvoicecontent_amtorder')

  d=b %>%
    gather(paytype_bin,paytype_order_cnt , -appl_no,-custorm_id) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.hourcount,paytype_order_cnt)
  names(d)[3:length(d)] <- paste0(gdhourcnt,'_meaninvoicecontent_amtorder')

  #2.合并变量框
  var_list=list(a,b,c,d)
  var = Reduce(full_join,var_list)
  names(var)[3:length(var)]=paste0('cust_fdesc_',names(var)[3:length(var)])

  #计算百分比变量
  input_1=var %>% ungroup() %>% select(contains('sum_invoicecontent')) %>% names()
  input_2=var %>% ungroup() %>% select(contains('sum_sum')) %>% names()
  input_3=paste0('cust_gddif_divide_',str_replace(str_replace(names(a)[3:length(a)],'sum_invoicecontent',''),'_amtorder',''),
                 'all','_sum_invoicecontent_amtorder')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))

  replacement=paste0('cust_',time_names,'_')
  names(var)=str_replace(names(var),'cust_',replacement)
  return(var)

}

fdesc_invoicecontent_amtorder=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_invoicecontent_amt(time_names = time_name)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
# fdesc_invoicecontent_amtorder(time_gap=180)

#----------------------------------------------------------------#





















####################################gddif_hourbin_cntorder.R##################
compute_crossvar_hourorder=function(df,name_1='2w',name_2='1m'){
  input1=df %>% ungroup() %>% select(contains(name_1)) %>%select(contains('fdesc_n_hour'))%>% names()
  input2=df %>% ungroup() %>% select(contains(name_2)) %>%select(contains('fdesc_n_hour'))%>% names()

  pattern1=paste0('cust_',name_1,'_fdesc')
  pattern2=input1 %>% str_replace(pattern1,'')
  pattern3=rep(c('minus','divide','difrate'),each=length(input1))
  input3=paste0('cust_',name_1,'_gddif_',pattern3, '_',name_1,name_2, rep(pattern2,len=3*length(pattern2)))
  str_exp=paste0(input3,'=',pattern3,'(',input1,',',input2,')',collapse = ',')

  text=paste0('df %>% mutate(',str_exp,')')
  vars=eval(parse(text=text))
  return(vars)
}

gddif_hourbin_cntorder=function(df){
  df%>%
    compute_crossvar_hourorder(name_1 = '2w',name_2 = '1m') %>%
    compute_crossvar_hourorder(name_1 = '2w',name_2 = '3m') %>%
    compute_crossvar_hourorder(name_1 = '2w',name_2 = '6m') %>%
    compute_crossvar_hourorder(name_1 = '2w',name_2 = '12m') %>%
    compute_crossvar_hourorder(name_1 = '2w',name_2 = 'all') %>%
    compute_crossvar_hourorder(name_1 = '1m',name_2 = '3m') %>%
    compute_crossvar_hourorder(name_1 = '1m',name_2 = '6m') %>%
    compute_crossvar_hourorder(name_1 = '1m',name_2 = '12m') %>%
    compute_crossvar_hourorder(name_1 = '1m',name_2 = 'all') %>%
    compute_crossvar_hourorder(name_1 = '3m',name_2 = '6m') %>%
    compute_crossvar_hourorder(name_1 = '3m',name_2 = '12m') %>%
    compute_crossvar_hourorder(name_1 = '3m',name_2 = 'all') %>%
    compute_crossvar_hourorder(name_1 = '6m',name_2 = '12m') %>%
    compute_crossvar_hourorder(name_1 = '6m',name_2 = 'all') %>%
    compute_crossvar_hourorder(name_1 = '12m',name_2 = 'all')
}
#-----------------------------------------------------------------#










####################################gddif_sts_cntorder.R###########################
compute_crossvar_stsorder=function(df,name_1='2w',name_2='1m'){
  input1=df %>% ungroup() %>% select(contains(name_1)) %>%select(contains('fdesc_n_sts'))%>% names()
  input2=df %>% ungroup() %>% select(contains(name_2)) %>%select(contains('fdesc_n_sts'))%>% names()

  pattern1=paste0('cust_',name_1,'_fdesc')
  pattern2=input1 %>% str_replace(pattern1,'')
  pattern3=rep(c('minus','divide','difrate'),each=length(input1))
  input3=paste0('cust_',name_1,'_gddif_',pattern3, '_',name_1,name_2, rep(pattern2,len=3*length(pattern2)))
  str_exp=paste0(input3,'=',pattern3,'(',input1,',',input2,')',collapse = ',')

  text=paste0('df %>% mutate(',str_exp,')')
  vars=eval(parse(text=text))
  return(vars)
}

gddif_sts_cntorder=function(df){
  df%>%
    compute_crossvar_stsorder(name_1 = '2w',name_2 = '1m') %>%
    compute_crossvar_stsorder(name_1 = '2w',name_2 = '3m') %>%
    compute_crossvar_stsorder(name_1 = '2w',name_2 = '6m') %>%
    compute_crossvar_stsorder(name_1 = '2w',name_2 = '12m') %>%
    compute_crossvar_stsorder(name_1 = '2w',name_2 = 'all') %>%
    compute_crossvar_stsorder(name_1 = '1m',name_2 = '3m') %>%
    compute_crossvar_stsorder(name_1 = '1m',name_2 = '6m') %>%
    compute_crossvar_stsorder(name_1 = '1m',name_2 = '12m') %>%
    compute_crossvar_stsorder(name_1 = '1m',name_2 = 'all') %>%
    compute_crossvar_stsorder(name_1 = '3m',name_2 = '6m') %>%
    compute_crossvar_stsorder(name_1 = '3m',name_2 = '12m') %>%
    compute_crossvar_stsorder(name_1 = '3m',name_2 = 'all') %>%
    compute_crossvar_stsorder(name_1 = '6m',name_2 = '12m') %>%
    compute_crossvar_stsorder(name_1 = '6m',name_2 = 'all') %>%
    compute_crossvar_stsorder(name_1 = '12m',name_2 = 'all')
}

#---------------------------------------------------#























#############################gddesc_jd618.R###############################
#指定时间段的全部数据
data_preparation_order=function(df,month=6,days_1=18,days_2=NA){
  if(is.na(days_2)){
    raw_data=df %>%
      select(appl_no,custorm_id,time_order,no_order,sts_order) %>%
      filter(month(time_order)==month & day(time_order)==days_1)
  }else{
    raw_data=df %>%
      select(appl_no,custorm_id,time_order,no_order,sts_order) %>%
      filter(month(time_order)==month & (day(time_order)>=days_1 & day(time_order)<=days_2))
  }
  return(raw_data)
}

#对应状态的数据，传入的DF为上述函数运行的结果集
data_preparation_sorder=function(df){
  df %>%
    filter(sts_order%in%c('完成','充值成功'))
}

data_preparation_corder=function(df){
  df %>%
    filter(sts_order%in%c('已取消','订单取消','已取消订单','配送退货'))
}

####################统计指标
desc.count.order=funs(sum,mean)
gdcntorder <- names(desc.count.order)
desc.count.order.indexes <- 3:(length(gdcntorder)+2)


compute_var_order=function(df,names='_jd618_'){

  text_exp="cust_jd618_gdesc_times_order=n_distinct(year(time_order)),cust_jd618_gdesc_exist_order=1,
  cust_jd618_gdesc_exist_sorder=if_else( n_distinct(case_when(sts_order %in% c('完成','充值成功') ~ 1),na.rm=T)>0 ,1,0) ,
  cust_jd618_gdesc_exist_corder=if_else( n_distinct(case_when(sts_order %in% c('已取消','订单取消','已取消订单','配送退货') ~ 1),na.rm=T)>0 ,1,0)
  "
  text_exp=str_replace_all(text_exp, '_jd618_', names)

  text=paste0('df %>% group_by(appl_no,custorm_id) %>% summarise(',text_exp,')')
  jd618_is_var=eval(parse(text=text))

  jd618_all_order_var=df %>%
    group_by(appl_no,custorm_id,year(time_order)) %>%
    summarise(n_618=n_distinct(no_order)) %>% #这里需要dist,原因是：no_order会由于商品而重复
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count.order,n_618)
  names(jd618_all_order_var)[desc.count.order.indexes]=paste0('cust',names,'gdesc_',gdcntorder,'_aorder')

  jd618_sorder_var=df %>%
    data_preparation_sorder()%>%
    group_by(appl_no,custorm_id,year(time_order)) %>%
    summarise(n_618=n_distinct(no_order)) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count.order,n_618)
  names(jd618_sorder_var)[desc.count.order.indexes]=paste0('cust',names,'gdesc_',gdcntorder,'_sorder')


  jd618_corder_var=df %>%
    data_preparation_corder() %>%
    group_by(appl_no,custorm_id,year(time_order)) %>%
    summarise(n_618=n_distinct(no_order)) %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count.order,n_618)
  names(jd618_corder_var)[desc.count.order.indexes]=paste0('cust',names,'gdesc_',gdcntorder,'_corder')

  #合并上述变量
  jd618_varorder_groups_list=list(
    jd618_is_var,
    jd618_all_order_var,
    jd618_sorder_var,
    jd618_corder_var
  )

  jd618_varorder_groups <- Reduce(full_join,jd618_varorder_groups_list)

  #计算交叉变量
  vars=compute_crossvar_order(jd618_varorder_groups,names=names)

  return(vars)
}


compute_crossvar_order=function(df,names='_jd618_'){

  input1=df %>% ungroup()%>%select(matches('[mn]_sorder')) %>% names()
  input2=df %>% ungroup()%>%select(matches('[mn]_corder')) %>% names()
  input3=df %>% ungroup()%>%select(contains('aorder')) %>% names()


  pattern1=paste0('cust',names,'gdesc')
  pattern2=rep(c('minus','divide','difrate'),each=length(input1))
  input4=paste0('cust',names,'gddif_',pattern2,'_sa',str_replace(input1,pattern1,''))
  str_exp1=paste0(input4,'=',pattern2,'(',input1,',',input3,')',collapse = ',')

  pattern2=rep(c('minus','divide','difrate'),each=length(input2))
  input5=paste0('cust',names,'gddif_',pattern2,'_ca',str_replace(input2,pattern1,''))
  str_exp2=paste0(input5,'=',pattern2,'(',input2,',',input3,')',collapse = ',')

  str_exp=paste(str_exp1,str_exp2,sep = ',')

  text=paste0('df %>% mutate(',str_exp,')')
  x=eval(parse(text=text))
  return(x)
}


#order_jd618_vars=df_order_product %>%
# data_preparation_order() %>%
#compute_var_order()

#order_jd618before_vars=df_order_product %>%
# data_preparation_order(month=6,days_1=1,days_2=17) %>%
#compute_var_order(names='_jd618before_')

##################################################################
data_preparation_amt=function(df,month=6,days_1=18,days_2=NA){
  if(is.na(days_2)){
    raw_data=df %>%
      select(appl_no,custorm_id,time_order,no_order,amt_order,sts_order) %>%
      filter(month(time_order)==month & day(time_order)==days_1) %>%
      filter(!is.na(amt_order))
  }else{
    raw_data=df %>%
      select(appl_no,custorm_id,time_order,no_order,amt_order,sts_order) %>%
      filter(month(time_order)==month & (day(time_order)>=days_1 & day(time_order)<=days_2)) %>%
      filter(!is.na(amt_order))
  }
  return(raw_data)
}

#对应状态的数据，传入的DF为上述函数运行的结果集
data_preparation_samt=function(df){
  df %>%
    filter(sts_order%in%c('完成','充值成功'))
}

data_preparation_camt=function(df){
  df %>%
    filter(sts_order%in%c('已取消','订单取消','已取消订单','配送退货'))
}


#计算逻辑
compute_var_amt=function(df,names='_jd618_'){


  jd618_all_order_var=df %>%
    select(appl_no,custorm_id,no_order,amt_order) %>%
    unique()  %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count,amt_order)
  names(jd618_all_order_var)[desc.count.indexes]=paste0('cust',names,'gdesc_',gdcnt,'_aamt')


  jd618_sorder_var=df %>%
    data_preparation_samt() %>%
    select(appl_no,custorm_id,no_order,amt_order) %>%
    unique()  %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count,amt_order)
  names(jd618_sorder_var)[desc.count.indexes]=paste0('cust',names,'gdesc_',gdcnt,'_samt')


  jd618_corder_var=df %>%
    data_preparation_camt() %>%
    select(appl_no,custorm_id,no_order,amt_order) %>%
    unique()  %>%
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count,amt_order)
  names(jd618_corder_var)[desc.count.indexes]=paste0('cust',names,'gdesc_',gdcnt,'_camt')

  #合并上述变量
  jd618_varorder_groups_list=list(
    jd618_all_order_var,
    jd618_sorder_var,
    jd618_corder_var
  )

  jd618_varorder_groups <- Reduce(full_join,jd618_varorder_groups_list)


  #计算交叉变量
  vars=compute_crossvar_amt(jd618_varorder_groups,names=names)

  return(vars)
}

compute_crossvar_amt=function(df,names='_jd618_'){
  input1=df %>% ungroup()%>%select(matches('_samt')) %>% names()
  input2=df %>% ungroup()%>%select(matches('_camt')) %>% names()
  input3=df %>% ungroup()%>%select(contains('aamt')) %>% names()


  pattern1=paste0('cust',names,'gdesc')
  pattern2=rep(c('minus','divide','difrate'),each=length(input1))
  input4=paste0('cust',names,'gddif_',pattern2,'_sa',str_replace(input1,pattern1,''))
  str_exp1=paste0(input4,'=',pattern2,'(',input1,',',input3,')',collapse = ',')

  pattern2=rep(c('minus','divide','difrate'),each=length(input2))
  input5=paste0('cust',names,'gddif_',pattern2,'_ca',str_replace(input2,pattern1,''))
  str_exp2=paste0(input5,'=',pattern2,'(',input2,',',input3,')',collapse = ',')

  str_exp=paste(str_exp1,str_exp2,sep = ',')

  text=paste0('df %>% mutate(',str_exp,')')
  x=eval(parse(text=text))
  return(x)
}

#df_order_product %>%
# data_preparation_amt(month=6,days_1=19,days_2=30) %>%
#compute_var_amt(names='_jd618after_')

##########################################################################
######################################商品数量（不去重）################

data_preparation_prd=function(df,month=6,days_1=18,days_2=NA){
  if(is.na(days_2)){
    raw_data=df %>%
      select(appl_no,custorm_id,time_order,no_order,product_id,sts_order) %>%
      filter(month(time_order)==month & day(time_order)==days_1)
  }else{
    raw_data=df %>%
      select(appl_no,custorm_id,time_order,no_order,product_id,sts_order) %>%
      filter(month(time_order)==month & (day(time_order)>=days_1 & day(time_order)<=days_2))
  }
  return(raw_data)
}

#对应状态的数据，传入的DF为上述函数运行的结果集
data_preparation_sprd=function(df){
  df %>%
    filter(sts_order%in%c('完成','充值成功'))
}

data_preparation_cprd=function(df){
  df %>%
    filter(sts_order%in%c('已取消','订单取消','已取消订单','配送退货'))
}


#计算逻辑
compute_var_prd=function(df,names='_jd618_'){


  jd618_all_order_var=df %>%
    group_by(appl_no,custorm_id,no_order) %>% unique()  %>%
    summarise(n_prd=n()) %>% #不去重
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count,n_prd)
  names(jd618_all_order_var)[desc.count.indexes]=paste0('cust',names,'gdesc_',gdcnt,'_aprd')

  jd618_sorder_var=df %>%
    data_preparation_sprd() %>%
    group_by(appl_no,custorm_id,no_order) %>% unique()  %>%
    summarise(n_prd=n()) %>% #不去重
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count,n_prd)
  names(jd618_sorder_var)[desc.count.indexes]=paste0('cust',names,'gdesc_',gdcnt,'_sprd')


  jd618_corder_var=df %>%
    data_preparation_cprd() %>%
    group_by(appl_no,custorm_id,no_order) %>% unique()  %>%
    summarise(n_prd=n()) %>% #不去重
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count,n_prd)
  names(jd618_corder_var)[desc.count.indexes]=paste0('cust',names,'gdesc_',gdcnt,'_cprd')

  #合并上述变量
  jd618_varorder_groups_list=list(
    jd618_all_order_var,
    jd618_sorder_var,
    jd618_corder_var
  )

  jd618_varorder_groups <- Reduce(full_join,jd618_varorder_groups_list)


  #计算交叉变量
  vars=compute_crossvar_prd(jd618_varorder_groups,names=names)

  return(vars)
}


#df为order部分的变量
compute_crossvar_prd=function(df,names='_jd618_'){
  input1=df %>% ungroup()%>%select(matches('_sprd')) %>% names()
  input2=df %>% ungroup()%>%select(matches('_cprd')) %>% names()
  input3=df %>% ungroup()%>%select(contains('aprd')) %>% names()


  pattern1=paste0('cust',names,'gdesc')
  pattern2=rep(c('minus','divide','difrate'),each=length(input1))
  input4=paste0('cust',names,'gddif_',pattern2,'_sa',str_replace(input1,pattern1,''))
  str_exp1=paste0(input4,'=',pattern2,'(',input1,',',input3,')',collapse = ',')

  pattern2=rep(c('minus','divide','difrate'),each=length(input2))
  input5=paste0('cust',names,'gddif_',pattern2,'_ca',str_replace(input2,pattern1,''))
  str_exp2=paste0(input5,'=',pattern2,'(',input2,',',input3,')',collapse = ',')

  str_exp=paste(str_exp1,str_exp2,sep = ',')

  text=paste0('df %>% mutate(',str_exp,')')
  x=eval(parse(text=text))
  return(x)
}

# df_order_product %>%
# data_preparation_prd(month=11,days_1=11) %>%head(1000)%>%
# compute_var_prd(names='_jd1111_')

#############################################商品数量（去重）###########
compute_var_distprd=function(df,names='_jd618_'){

  jd618_all_order_var=df %>%
    group_by(appl_no,custorm_id,no_order) %>% unique()  %>%
    summarise(n_distprd=n_distinct(product_id)) %>% #去重
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count,n_distprd)
  names(jd618_all_order_var)[desc.count.indexes]=paste0('cust',names,'gdesc_',gdcnt,'_distaprd')

  jd618_sorder_var=df %>%
    data_preparation_sprd() %>%
    group_by(appl_no,custorm_id,no_order) %>% unique()  %>%
    summarise(n_distprd=n_distinct(product_id)) %>% #去重
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count,n_distprd)
  names(jd618_sorder_var)[desc.count.indexes]=paste0('cust',names,'gdesc_',gdcnt,'_distsprd')


  jd618_corder_var=df %>%
    data_preparation_cprd() %>%
    group_by(appl_no,custorm_id,no_order) %>% unique()  %>%
    summarise(n_distprd=n_distinct(product_id)) %>% #去重
    group_by(appl_no,custorm_id) %>%
    summarise_each(desc.count,n_distprd)
  names(jd618_corder_var)[desc.count.indexes]=paste0('cust',names,'gdesc_',gdcnt,'_distcprd')

  #合并上述变量
  jd618_varorder_groups_list=list(
    jd618_all_order_var,
    jd618_sorder_var,
    jd618_corder_var
  )

  jd618_varorder_groups <- Reduce(full_join,jd618_varorder_groups_list)


  #计算交叉变量
  vars=compute_crossvar_distprd(jd618_varorder_groups,names=names)

  return(vars)
}


#df为order部分的变量
compute_crossvar_distprd=function(df,names='_jd618_'){
  input1=df %>% ungroup()%>%select(matches('_distsprd')) %>% names()
  input2=df %>% ungroup()%>%select(matches('_distcprd')) %>% names()
  input3=df %>% ungroup()%>%select(contains('distaprd')) %>% names()


  pattern1=paste0('cust',names,'gdesc')
  pattern2=rep(c('minus','divide','difrate'),each=length(input1))
  input4=paste0('cust',names,'gddif_',pattern2,'_sa',str_replace(input1,pattern1,''))
  str_exp1=paste0(input4,'=',pattern2,'(',input1,',',input3,')',collapse = ',')

  pattern2=rep(c('minus','divide','difrate'),each=length(input2))
  input5=paste0('cust',names,'gddif_',pattern2,'_ca',str_replace(input2,pattern1,''))
  str_exp2=paste0(input5,'=',pattern2,'(',input2,',',input3,')',collapse = ',')

  str_exp=paste(str_exp1,str_exp2,sep = ',')

  text=paste0('df %>% mutate(',str_exp,')')
  x=eval(parse(text=text))
  return(x)
}

#df_order_product %>%
# data_preparation_prd(month=11,days_1=11) %>%
#compute_var_distprd(names='_jd1111_')

#######################################################
#函数封装，供最后调用及并行化处理
#######################################################
gddesc_jd618=function(df){
  order_jd618_vars=df %>%data_preparation_order() %>% compute_var_order()

  order_jd618before_vars=df %>% data_preparation_order(month=6,days_1=1,days_2=17) %>%
    compute_var_order(names='_jd618before_')

  order_jd618after_vars=df %>%data_preparation_order(month=6,days_1=19,days_2=30) %>%
    compute_var_order(names='_jd618after_')
  #------------------------------------------------#
  amt_jd618_vars=df %>%data_preparation_amt() %>%compute_var_amt()

  amt_jd618before_vars=df %>%data_preparation_amt(month=6,days_1=1,days_2=17) %>%
    compute_var_amt(names='_jd618before_')

  amt_jd618after_vars=df %>%data_preparation_amt(month=6,days_1=19,days_2=30) %>%
    compute_var_amt(names='_jd618after_')

  #----------------------------------------------#
  prd_jd618_vars=df %>%data_preparation_prd() %>%
    compute_var_prd()

  prd_jd618before_vars=df %>%data_preparation_prd(month=6,days_1=1,days_2=17) %>%
    compute_var_prd(names='_jd618before_')

  prd_jd618after_vars=df %>%data_preparation_prd(month=6,days_1=19,days_2=30) %>%
    compute_var_prd(names='_jd618after_')

  #-----------------------------------------------#
  distprd_jd618_vars=df %>%data_preparation_prd() %>%
    compute_var_distprd()

  distprd_jd618before_vars=df %>%data_preparation_prd(month=6,days_1=1,days_2=17) %>%
    compute_var_distprd(names='_jd618before_')

  distprd_jd618after_vars=df %>%data_preparation_prd(month=6,days_1=19,days_2=30) %>%
    compute_var_distprd(names='_jd618after_')

  #----------------------------------------------#

  vars_jd6i8_list=list(
    order_jd618_vars,
    order_jd618before_vars,
    order_jd618after_vars,
    amt_jd618_vars,
    amt_jd618before_vars,
    amt_jd618after_vars,
    prd_jd618_vars,
    prd_jd618before_vars,
    prd_jd618after_vars,
    distprd_jd618_vars,
    distprd_jd618before_vars,
    distprd_jd618after_vars
  )

  vars_jd618=Reduce(full_join,vars_jd6i8_list)
  vars_jd618=compute_var_sumtimes(vars_jd618,times='618')
  return(vars_jd618)
}


gddesc_jd1111=function(df){
  order_jd1111_vars=df %>%
    data_preparation_order(month=11,days_1=11) %>%
    compute_var_order(names='_jd1111_')

  order_jd1111before_vars=df %>%
    data_preparation_order(month=11,days_1=1,days_2=10) %>%
    compute_var_order(names='_jd1111before_')

  order_jd1111after_vars=df %>%
    data_preparation_order(month=11,days_1=12,days_2=30) %>%
    compute_var_order(names='_jd1111after_')
  #------------------------------------------------#
  amt_jd1111_vars=df %>%
    data_preparation_amt(month=11,days_1=11) %>%
    compute_var_amt(names='_jd1111_')

  amt_jd1111before_vars=df %>%
    data_preparation_amt(month=11,days_1=1,days_2=10) %>%
    compute_var_amt(names='_jd1111before_')

  amt_jd1111after_vars=df %>%
    data_preparation_amt(month=11,days_1=12,days_2=30) %>%
    compute_var_amt(names='_jd1111after_')

  #----------------------------------------------#
  prd_jd1111_vars=df %>%
    data_preparation_prd(month=11,days_1=11) %>%
    compute_var_prd(names='_jd1111_')

  prd_jd1111before_vars=df %>%
    data_preparation_prd(month=11,days_1=1,days_2=10) %>%
    compute_var_prd(names='_jd1111before_')

  prd_jd1111after_vars=df %>%
    data_preparation_prd(month=11,days_1=12,days_2=30) %>%
    compute_var_prd(names='_jd1111after_')

  #-----------------------------------------------#
  distprd_jd1111_vars=df %>%
    data_preparation_prd(month=11,days_1=11) %>%
    compute_var_distprd(names='_jd1111_')

  distprd_jd1111before_vars=df %>%
    data_preparation_prd(month=11,days_1=1,days_2=10) %>%
    compute_var_distprd(names='_jd1111before_')

  distprd_jd1111after_vars=df %>%
    data_preparation_prd(month=11,days_1=12,days_2=30) %>%
    compute_var_distprd(names='_jd1111after_')

  #----------------------------------------------#

  vars_jd1111_list=list(
    order_jd1111_vars,
    order_jd1111before_vars,
    order_jd1111after_vars,
    amt_jd1111_vars,
    amt_jd1111before_vars,
    amt_jd1111after_vars,
    prd_jd1111_vars,
    prd_jd1111before_vars,
    prd_jd1111after_vars,
    distprd_jd1111_vars,
    distprd_jd1111before_vars,
    distprd_jd1111after_vars
  )


  vars_jd1111=Reduce(full_join,vars_jd1111_list)
  vars_jd1111=compute_var_sumtimes(vars_jd1111,times='1111')
  return(vars_jd1111)
}


#####################三个时间段汇总变量#############
compute_var_sumtimes=function(df,times='618'){
  str_order=df[1,] %>% ungroup() %>%select(contains('sum')) %>%
    select(contains('_aorder')) %>% names() %>% paste0(collapse =',')
  str_amt=df[1,] %>% ungroup()%>% select(contains('sum')) %>%
    select(contains('_aamt')) %>% names() %>% paste0(collapse =',')
  str_prd=df[1,] %>% ungroup()%>% select(contains('sum')) %>%
    select(contains('_aprd')) %>% names() %>% paste0(collapse =',')
  str_distprd=df[1,] %>% ungroup()%>%select(contains('sum')) %>%
    select(contains('_distaprd')) %>% names()%>% paste0(collapse =',')
  str_exp=paste(
    paste0('cust_jd618sum_gdesc_order=','sum(' , str_order , ',na.rm=T)'),
    paste0('cust_jd618sum_gdesc_amt=','sum(' , str_amt , ',na.rm=T)'),
    paste0('cust_jd618sum_gdesc_prd=','sum(' , str_prd , ',na.rm=T)'),
    paste0('cust_jd618sum_gdesc_distprd=','sum(' , str_distprd , ',na.rm=T)'),
    paste0('cust_jd618sum_gddif_divide_normtimes_order=',
           'divide(cust_jd618sum_gdesc_order,cust_jd618_gdesc_times_order )'),
    paste0('cust_jd618sum_gddif_divide_normtimes_amt=',
           'divide(cust_jd618sum_gdesc_amt,cust_jd618_gdesc_times_order )'),
    paste0('cust_jd618sum_gddif_divide_normtimes_prd=',
           'divide(cust_jd618sum_gdesc_prd,cust_jd618_gdesc_times_order )'),
    paste0('cust_jd618sum_gddif_divide_normtimes_distprd=',
           'divide(cust_jd618sum_gdesc_distprd,cust_jd618_gdesc_times_order )'),
    sep =','
  )
  str_exp=str_replace_all(str_exp,'618',times)
  text=paste0('df %>% group_by(appl_no,custorm_id) %>% mutate(',str_exp,')')
  jdtimes_var=eval(parse(text=text))
  return(jdtimes_var)
}




#################################################################################
#计算 过年的变量
#################################################################################

data_preparation_springfestival=function(df,...){
  raw_data=df %>%
    select(appl_no,custorm_id,time_order,no_order,sts_order,...)%>%
    filter(month(time_order) %in% c(12,1) )
  return(raw_data)
}

#df为原始数据
compute_var_springfestival=function(df){
  vars_springfestival_list=list(
    data_preparation_springfestival(df) %>% compute_var_order(names='_springfestival_'),
    data_preparation_springfestival(df,amt_order) %>% compute_var_amt(names='_springfestival_'),
    data_preparation_springfestival(df,product_id) %>% compute_var_prd(names='_springfestival_'),
    data_preparation_springfestival(df,product_id) %>% compute_var_distprd(names='_springfestival_')
  )
  vars_springfestival=Reduce(full_join,vars_springfestival_list)
  return(vars_springfestival)
}

gddesc_springfestival=function(df){
  compute_var_springfestival(df)
}

#测试
#df_order_product%>%filter(appl_no=='20160813110005997260') %>%
# gddesc_springfestival()

#---------------------------------------------------------------------------#





















#############################################gddesc_loantime_now.R##############

compute_var_time=function(df){
  df_time=df %>%
    select(appl_no,custorm_id, time_order,appl_sbm_tm) %>%
    mutate(hour_gap=difftime(appl_sbm_tm,time_order,units='hours'),
           day_gap=difftime(appl_sbm_tm,time_order,units='days'),
           week_gap=difftime(appl_sbm_tm,time_order,units='weeks'),
           month_gap=divide(as.numeric(day_gap),30)
    ) %>%
    group_by(appl_no,custorm_id) %>%
    summarise(cust_all_gddesc_minus_loantimenow_max_daydiff=as.numeric(max(day_gap)),#最早下单时间与贷款时间的差
              cust_all_gddesc_minus_loantimenow_max_weekdiff=as.numeric(max(week_gap)),
              cust_all_gddesc_minus_loantimenow_max_monthdiff=as.numeric(max(month_gap)),

              cust_all_gddesc_minus_loantimenow_min_hourdiff=as.numeric(min(hour_gap)),
              cust_all_gddesc_minus_loantimenow_min_daydiff=as.numeric(min(day_gap)), #最晚下单时间与贷款时间的差
              cust_all_gddesc_minus_loantimenow_min_weekdiff=as.numeric(min(week_gap)),
              cust_all_gddesc_minus_loantimenow_min_monthdiff=as.numeric(min(month_gap))
    )
  return(df_time)
}

gddesc_loantime_now=function(df){
  compute_var_time(df)
}

# a=df_order_product%>%filter(appl_no=='20160813110005997260') %>% compute_var_time()
# View(a)
#
# df_order_product%>%filter(appl_no=='20160813110005997260') %>%gddesc_loantime_now()

#----------------------------------------#







##########################fdesc_prdname_prdcategory.R#######################
desc_prd_name=c("phonedigital","computeroffice","travelrecharge","clothing",
                "foodswine","motherbabytoys","sportsoutdoor","makeupsuppliespet" ,
                "shoesbagsjewelry","homefurniture","automotiveproducts","medicinehealth" ,
                "domesticappliance","booksvideo","financial" ,"gender",
                "colour" ,"quantity" ,"discount","specialcharacter")

##############################################
#df为含有中间变量的数据
compute_var_prdname=function(df,time_names='2w'){
  a=df%>%select(appl_no,custorm_id,phonedigital,computeroffice,travelrecharge,clothing,foodswine,motherbabytoys,
                sportsoutdoor,makeupsuppliespet,shoesbagsjewelry,homefurniture,automotiveproducts,
                medicinehealth,domesticappliance,booksvideo,financial,
                gender,colour,quantity,discount,specialcharacter)%>%
    group_by(appl_no,custorm_id)%>%
    summarise_all(funs(sum(.,na.rm=T)))

  #names(a)[3:length(a)]=paste0('cust_',time_names,'_fdesc_n_',names(desc.prd_name),'_cntprdcategory')
  #解耦合
  names(a)[3:length(a)]=paste0('cust_',time_names,'_fdesc_n_',desc_prd_name,'_cntprdcategory')

  b=df%>%select(appl_no,custorm_id,prd_name)%>%group_by(appl_no,custorm_id)%>%summarise(all=n())
  var=full_join(a,b)

  input1=names(a)[3:17]
  input2='all'
  input3=paste0('cust_',time_names,'_gddif_divide_',desc_prd_name[1:15],'all','_n_cntprdcategory')
  str_exp=paste0(input3,'=','divide(',input1,',',input2,')',collapse = ',')
  text=paste0('var %>% mutate(',str_exp,')')
  var=eval(parse(text=text))
  var=var%>%ungroup()%>%select(appl_no,starts_with('cust'))
  return(var)
}

#a%>%compute_var_prdname()

#此处的df为含有中间字段的df
fdesc_prdname_prdcategory=function(df,time_gap=14){
  timebin=c(14,30,90,180,360,Inf)
  timenamesbin=c('2w','1m','3m','6m','12m','all')
  time_name=timenamesbin[which(timebin==time_gap)]
  df %>% data_preparation_time(time_gaps=time_gap) %>% compute_var_prdname(time_names = time_name)
}

#df_order_product%>%head(0)%>%fdesc_prdname_prdcategory()

#-------------------------------------------------------------------#




#######################################
#以上部分为计算order_product部分的函数#
#######################################











#####################################fdesc_auth_info.R###################
cust_fddesc_exist_channelwallet_auth=function(channel){
  if_else(!is.na(channel),if_else(channel=='京东钱包实名认证',1,0),0)
}
cust_fddesc_exist_channelfinance_auth=function(channel){
  if_else(!is.na(channel),if_else(channel=='京东金融实名认证',1,0),0)
}
cust_fddesc_exist_channelnull_auth=function(channel){
  if_else(is.na(channel),1,0)
}

desc.channel=funs(cust_fddesc_exist_channelwallet_auth,cust_fddesc_exist_channelfinance_auth,
                  cust_fddesc_exist_channelnull_auth)

compute_var_channel=function(df){
  df%>%group_by(appl_no,custorm_id)%>%summarise_each(desc.channel,channel)
}

# a=auth%>% head(10)%>%compute_var_channel()
# View(a)

cust_fddesc_exist_finanserv_ious=function(finan_serv){
  if_else(!is.na(finan_serv),if_else(!is.na(str_match(finan_serv,'白条')),1,0),0)
}

cust_fddesc_exist_finanserv_null=function(finan_serv){
  if_else(is.na(finan_serv),1,0)
}

desc.finanserv=funs(cust_fddesc_exist_finanserv_ious,cust_fddesc_exist_finanserv_null)

compute_var_finanserv=function(df){
  df%>%group_by(appl_no,custorm_id)%>%summarise_each(desc.finanserv,finan_serv)
}

# a=auth%>% head(10)%>%compute_var_finanserv()

cust_fddesc_exist_loginname_phone=function(login_name){
  phone='[1][0-9]{10}'
  n=sum(if_else(is.na(str_match(login_name,phone)),0,1))
  return(if_else(n>0,1,0))
}

cust_fddesc_exist_loginname_email=function(login_name){
  email = "[\\w-\\.]+@[\\w]+\\.+[a-z]{2,3}"
  n=sum(if_else(is.na(str_match(login_name,email)),0,1))
  return(if_else(n>0,1,0))
}


cust_fddesc_exist_loginname_logname=function(login_name){
  logname="[a-zA-z]+"
  n=sum(if_else(is.na(str_match(login_name,logname)),0,1))
  return(if_else(n>0,1,0))
}


cust_fddesc_exist_loginname_lognamenumb=function(login_name){
  lognameNumb="[a-zA-z_]+[0-9]+"
  n=sum(if_else(is.na(str_match(login_name,lognameNumb)),0,1))
  return(if_else(n>0,1,0))
}

cust_fddesc_exist_loginname_chinese=function(login_name){
  chinese="[\u4e00-\u9fa5]+"
  n=sum(if_else(is.na(str_match(login_name,chinese)),0,1))
  return(if_else(n>0,1,0))
}

cust_fddesc_exist_loginname_chinesenumb=function(login_name){
  chineseNumb="[\u4e00-\u9fa5]+[\\w]+"
  n=sum(if_else(is.na(str_match(login_name,chineseNumb)),0,1))
  return(if_else(n>0,1,0))
}

cust_fddesc_exist_loginname_jdname=function(login_name){
  jdname="jd_[\\w]+"
  n=sum(if_else(is.na(str_match(login_name,jdname)),0,1))
  return(if_else(n>0,1,0))
}

desc.loginname_exist=funs(cust_fddesc_exist_loginname_phone,cust_fddesc_exist_loginname_email,
                          cust_fddesc_exist_loginname_logname,cust_fddesc_exist_loginname_lognamenumb,
                          cust_fddesc_exist_loginname_chinese,cust_fddesc_exist_loginname_chinesenumb,
                          cust_fddesc_exist_loginname_jdname)

compute_var_loginname=function(df){
  df%>%group_by(appl_no,custorm_id)%>%summarise_each(desc.loginname_exist,login_name)
}


#auth%>% head(10)%>%compute_var_loginname()


#如果loginname是手机号，能否匹配和phone匹配上
cust_fddesc_exist_loginname_equal_phone=function(login_name,phone){

  phonepattern='[1][0-9]{10}'
  n=if_else(!is.na(str_match(login_name,phonepattern)),
            if_else(is_match(login_name,phone),1,0),0)
  return(n)
}

is_match=function(a,b){
  phonepattern='[1][0-9]{10}'
  str=str_extract(a,phonepattern)  #1.提取出电话号码
  str=paste0(substr(str,1,3),'****',substr(str,8,11)) #2.将login_name中的电话号码按phone的规则脱敏
  if_else((str==b&!is.na(a)&!is.na(b)),TRUE,FALSE)
}


compute_var_loginname_equal_phone=function(df){
  df%>%group_by(appl_no,custorm_id)%>%summarise(
    cust_fddesc_exist_loginname_equal_phone=cust_fddesc_exist_loginname_equal_phone(login_name,phone)
  )
}

# a=auth%>% head(100)%>%compute_var_loginname_equal_phone()
#
# auth%>%filter(appl_no=='20160502140002648932')%>%select(login_name,phone)


cust_fddesc_exist_realname_null=function(real_name){
  if_else(is.na(real_name),1,0)
}

cust_fddesc_exist_realname_pattern=function(real_name){
  if_else(!is.na(real_name),
          if_else(!is.na(str_match(real_name,'[a-zA-Z_0-9]')),1,0),0)
}

desc.realname_exist=funs(cust_fddesc_exist_realname_null,cust_fddesc_exist_realname_pattern)
compute_var_realname=function(df){
  df%>%group_by(appl_no,custorm_id)%>%summarise_each(
    desc.realname_exist,real_name
  )
}

# auth%>% head(100)%>%compute_var_realname()
####申请时间-认证时间  如果是负，赋值为-1

####新加一个变量：是否在申请前实名认证了（1：在申请前就实名认证了 ） #20161111

cust_fddesc_minus_authtimenow_seconds=function(appl_sbm_tm,auth_time){
  auth_time=as.POSIXct(auth_time,format="%Y-%m-%d")
  appl_sbm_tm=as.POSIXct(appl_sbm_tm,format="%Y-%m-%d %H:%M:%S")
  as.numeric(difftime(appl_sbm_tm,auth_time,units = "secs"))
}

compute_var_authtimenow=function(df){
  df%>%group_by(appl_no,custorm_id)%>%summarise(
    cust_fddesc_minus_nowauthtime_seconds=cust_fddesc_minus_authtimenow_seconds(appl_sbm_tm,auth_time),
    cust_fddesc_minus_nowauthtime_negative=if_else(cust_fddesc_minus_nowauthtime_seconds<0,1,0),
    ##1208,小峰讨论后新增的衍生变量
    cust_fddesc_minus_nowauthtime_isna = if_else(is.na(cust_fddesc_minus_nowauthtime_seconds),1,0)
  )
}

#a=auth%>% head(100)%>%compute_var_authtimenow()

fdesc_auth_info=function(df){
  df=df%>%filter(!is.na(custorm_id))
  var_list=list(
    compute_var_channel(df),compute_var_finanserv(df),
    compute_var_loginname(df),compute_var_loginname_equal_phone(df),
    compute_var_realname(df),compute_var_authtimenow(df)
  )
  var=Reduce(full_join,var_list)
  names(var) <- str_replace(names(var),pattern = 'cust_',replacement = 'cust_jdauth_')
  return(var)
}

#fdesc_auth_info(df_auth%>% head(100)) 19列 #1208修改后，新增了一列，变为20列

#------------------------------------------------------------------#










#######################################fdesc_user_info#######################
cust_fddesc_exist_loginname_phone=function(login_name){
  phone='[1][0-9]{10}'
  n=sum(if_else(is.na(str_match(login_name,phone)),0,1))
  return(if_else(n>0,1,0))
}

cust_fddesc_exist_loginname_email=function(login_name){
  email = "[\\w-\\.]+@[\\w]+\\.+[a-z]{2,3}"
  n=sum(if_else(is.na(str_match(login_name,email)),0,1))
  return(if_else(n>0,1,0))
}


cust_fddesc_exist_loginname_logname=function(login_name){
  logname="[a-zA-z]+"
  n=sum(if_else(is.na(str_match(login_name,logname)),0,1))
  return(if_else(n>0,1,0))
}


cust_fddesc_exist_loginname_lognamenumb=function(login_name){
  lognameNumb="[a-zA-z_]+[0-9]+"
  n=sum(if_else(is.na(str_match(login_name,lognameNumb)),0,1))
  return(if_else(n>0,1,0))
}

cust_fddesc_exist_loginname_chinese=function(login_name){
  chinese="[\u4e00-\u9fa5]+"
  n=sum(if_else(is.na(str_match(login_name,chinese)),0,1))
  return(if_else(n>0,1,0))
}

cust_fddesc_exist_loginname_chinesenumb=function(login_name){
  chineseNumb="[\u4e00-\u9fa5]+[\\w]+"
  n=sum(if_else(is.na(str_match(login_name,chineseNumb)),0,1))
  return(if_else(n>0,1,0))
}

cust_fddesc_exist_loginname_jdname=function(login_name){
  jdname="jd_[\\w]+"
  n=sum(if_else(is.na(str_match(login_name,jdname)),0,1))
  return(if_else(n>0,1,0))
}

desc.loginname_exist=funs(cust_fddesc_exist_loginname_phone,cust_fddesc_exist_loginname_email,
                          cust_fddesc_exist_loginname_logname,cust_fddesc_exist_loginname_lognamenumb,
                          cust_fddesc_exist_loginname_chinese,cust_fddesc_exist_loginname_chinesenumb,
                          cust_fddesc_exist_loginname_jdname)



compute_var_aboutnames=function(df){
  df=df%>% tbl_df() %>%filter(!is.na(custorm_id))%>%select(appl_no,custorm_id,login_name,web_login_name,user_name)
  a=df%>%group_by(appl_no,custorm_id)%>%summarise_each(desc.loginname_exist,login_name)
  b=df%>%group_by(appl_no,custorm_id)%>%summarise_each(desc.loginname_exist,web_login_name)
  names(b)[3:length(b)]=str_replace(names(b)[3:length(b)],'loginname','webloginname')
  c=df%>%group_by(appl_no,custorm_id)%>%summarise_each(desc.loginname_exist,user_name)
  names(c)[3:length(c)]=str_replace(names(c)[3:length(c)],'loginname','username')
  d=df%>%group_by(appl_no,custorm_id)%>%
    summarise(cust_fddesc_nd_compare_threenames=n_distinct(c(login_name,web_login_name,user_name)))
  var=Reduce(full_join,list(a,b,c,d))
  names(var) <- str_replace(names(var),pattern = 'cust_',replacement = 'cust_jduser_')
  return(var)
}


var_jd_user <- function(jd_user_rightjoin_appno){
  #load("/home/datashare/crawl_data/基础工具/crawler_jd_mapto_ms_application_dim1.RData")


  jd_user<- jd_user_rightjoin_appno%>%filter(!is.na(rowkey)) %>%
    select(appl_no,custorm_id,sex,birthday,web_login_name,user_name,hobby,email,real_name,
           merriage,income,degree,industry,qq_bound,wechat_bound,account_grade,account_type) %>%data.frame()


  ########数据预处理
  ######京东的生日是自己选的，最小是1930-01-01，最大是当前日期
  ### 所以 c('0000-00-00','0001-01-01') 是系统异常值 与用户无关 直接处理为缺失值即可
  jd_user[,'birthday'][jd_user[,'birthday']%in%c('0000-00-00','0001-01-01')] <- NA



  jd_user_addcol <- jd_user%>%merge(crawler_jd_mapto_ms_application_dim1%>%filter(map_key=="user.sex")%>%select(key,value)%>%rename(sex=key),all.x = T)%>%rename(jd_gender_cd=value)
  jd_user_addcol <- jd_user_addcol%>%merge(crawler_jd_mapto_ms_application_dim1%>%filter(map_key=="user.marriage")%>%select(key,value)%>%rename(merriage=key),all.x = T)%>%rename(jd_marriage_chr=value)
  jd_user_addcol <- jd_user_addcol%>%merge(crawler_jd_mapto_ms_application_dim1%>%filter(map_key=="user.industry")%>%select(key,value)%>%rename(industry=key),all.x = T)%>%rename(jd_industry_chr=value)
  jd_user_addcol <- jd_user_addcol%>%merge(crawler_jd_mapto_ms_application_dim1%>%filter(map_key=="user.education")%>%select(key,value)%>%rename(degree=key),all.x = T)%>%rename(jd_degree_chr=value)




  cust_hobby_bookaudiovideodigitalproducts <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '图书/音像/数字商品'))
  cust_hobby_householdelectricappliance <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '家用电器'))
  cust_hobby_mobilephonedigital <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '手机/数码'))
  cust_hobby_computeroffice <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '电脑/办公'))
  cust_hobby_homefurniturekitchenappliances <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '家居/家具/家装/厨具'))
  cust_hobby_dressunderwearjewelry <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '服饰内衣/珠宝首饰'))
  cust_hobby_personalcaremakeup <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '个护化妆'))
  cust_hobby_shoebagswatchluxury <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '鞋靴/箱包/钟表/奢侈品'))
  cust_hobby_sportshealth <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '运动健康'))
  cust_hobby_caraccessory <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '汽车用品'))
  cust_hobby_maternalchildtoymusicalinstruments <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '母婴/玩具乐器'))
  cust_hobby_foodbeveragehealthfood <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '食品饮料/保健食品'))
  cust_hobby_lotterytravelrechargeticket <- as.numeric(str_detect(jd_user_addcol[,'hobby'],pattern = '食品饮料/保健食品'))
  cust_isna_email <- as.numeric(is.na(jd_user_addcol[,'email']))

  cust_isemail_qq <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]qq\\.'))
  cust_isemail_163 <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]163\\.'))
  cust_isemail_126 <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]126\\.'))
  cust_isemail_sina <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]sina\\.'))
  cust_isemail_139 <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]139\\.'))
  cust_isemail_hotmail <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]hotmail\\.'))
  cust_isemail_foxmail <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]foxmail\\.'))
  cust_isemail_189 <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]189\\.'))
  cust_isemail_sohu <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]sohu\\.'))
  cust_isemail_gmail <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]gmail\\.'))
  cust_isemail_yahoo <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]yahoo\\.'))
  cust_isemail_aliyun <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]aliyun\\.'))
  cust_isemail_yeah <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]yeah\\.'))
  cust_isemail_live <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]live\\.'))
  cust_isemail_outlook <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]outlook\\.'))
  cust_isemail_icloud <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@.]icloud\\.'))
  cust_isvipemail <- as.numeric(str_detect(tolower(jd_user_addcol[,'email']),pattern = '[@]vip\\.'))
  cust_isemail_other <-  as.numeric(ifelse((cust_isemail_126==0)&(cust_isemail_139==0)&(cust_isemail_163==0)&(cust_isemail_189==0)&(cust_isemail_aliyun==0)&(cust_isemail_foxmail==0)&(cust_isemail_gmail==0)&(cust_isemail_hotmail==0)&(cust_isemail_icloud==0)&(cust_isemail_live==0)&(cust_isemail_outlook==0)&(cust_isemail_qq==0)&(cust_isemail_sina==0)&(cust_isemail_sohu==0)&(cust_isemail_yahoo==0)&(cust_isemail_yeah==0),1,0))

  cust_isna_realname <- as.numeric(  is.na(jd_user_addcol[,'real_name']) )

  cust_isna_marriage <-  as.numeric(is.na(jd_user_addcol[,'merriage']) )

  cust_onehot_income_4000_5999 <- as.numeric(jd_user_addcol[,'income']=='4000-5999元')
  cust_onehot_income_0000_1999 <- as.numeric(jd_user_addcol[,'income']=='2000元以下')

  cust_onehot_income_6000_7999 <- as.numeric(jd_user_addcol[,'income']=='6000-7999元')

  cust_onehot_income_8000_inf <- as.numeric(jd_user_addcol[,'income']=='8000元以上')

  cust_isna_income <- as.numeric(is.na(jd_user_addcol[,'income']))

  cust_onehot_degree_a   <- as.numeric(jd_user_addcol[,'jd_degree_chr']=='A')
  cust_onehot_degree_b   <- as.numeric(jd_user_addcol[,'jd_degree_chr']=='B')
  cust_onehot_degree_c   <- as.numeric(jd_user_addcol[,'jd_degree_chr']=='C')
  cust_onehot_degree_d   <- as.numeric(jd_user_addcol[,'jd_degree_chr']=='D')
  cust_onehot_degree_e   <- as.numeric(jd_user_addcol[,'jd_degree_chr']=='E')
  cust_onehot_degree_f   <- as.numeric(jd_user_addcol[,'jd_degree_chr']=='F')

  cust_isna_degree <- as.numeric(is.na(jd_user_addcol[,'degree']))

  cust_onehot_industry_a   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='A')
  cust_onehot_industry_b   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='B')
  cust_onehot_industry_c   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='C')
  cust_onehot_industry_d   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='D')
  cust_onehot_industry_e   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='E')
  cust_onehot_industry_f   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='F')
  cust_onehot_industry_g   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='G')
  cust_onehot_industry_h   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='H')
  cust_onehot_industry_i   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='I')
  cust_onehot_industry_j   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='J')
  cust_onehot_industry_l   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='L')
  cust_onehot_industry_m   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='M')
  cust_onehot_industry_n   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='N')
  cust_onehot_industry_o   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='O')
  cust_onehot_industry_p   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='P')
  cust_onehot_industry_q   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='Q')
  cust_onehot_industry_r   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='R')
  cust_onehot_industry_s   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='S')
  cust_onehot_industry_z   <- as.numeric(jd_user_addcol[,'jd_industry_chr']=='Z')

  cust_isna_industry <- as.numeric(is.na(jd_user_addcol[,'jd_industry_chr']))

  cust_isbind_qq <- ifelse(jd_user_addcol[,'qq_bound']=="已绑定",1,0)
  cust_isbind_wechat <- ifelse(jd_user_addcol[,'wechat_bound']=="已绑定",1,0)
  cust_isbind_bothqqwechat <- pmax(cust_isbind_qq,cust_isbind_wechat)

  cust_onehot_acctgrade_diamond   <- as.numeric(jd_user_addcol[,'account_grade']=='钻石会员')
  cust_onehot_acctgrade_gold      <- as.numeric(jd_user_addcol[,'account_grade']=='金牌会员')
  cust_onehot_acctgrade_silver   <- as.numeric(jd_user_addcol[,'account_grade']=='银牌会员')
  cust_onehot_acctgrade_copper   <- as.numeric(jd_user_addcol[,'account_grade']=='铜牌会员')
  cust_onehot_acctgrade_register   <- as.numeric(jd_user_addcol[,'account_grade']=='注册会员')

  cust_isacct_personal <- ifelse(jd_user_addcol[,'account_type']=="个人用户",1,0)
  ls <- ls()
  varnames <- ls[str_detect(ls,'cust_')]
  text=paste0('jduser_vars <- data_frame(appl_no=jd_user$appl_no,custorm_id=jd_user$custorm_id,',paste0(varnames,collapse = ','),')')
  eval(parse(text = text))

  names(jduser_vars) <- str_replace(names(jduser_vars),pattern = 'cust_',replacement = 'cust_jduser_') #单表变量不标记单表

  jduser_vars %>%tbl_df()

}


fdesc_user_info=function(df){
  ls=list(compute_var_aboutnames(df),var_jd_user(df))
  jduser_vars=Reduce(full_join,ls)
  return(jduser_vars)
}


#fdesc_user_info(user%>%head(0)) 98列

#--------------------------------------------------------------------------------#











#####################fdesc_bt_info.R#########################
fdesc_bt_info=function(df){
  #数据预处理
  df=df%>%tbl_df()%>%filter(!is.na(custorm_id))
  df$credit_score=as.numeric(df$credit_score)
  df$overdraft=as.numeric(df$overdraft)
  df$quota=as.numeric(df$quota)
  options(digits = 7)

  var=df%>%group_by(appl_no,custorm_id)%>%
    summarise(cust_gddesc_extract_creditscore_bt=credit_score,
              cust_gddesc_extract_quota_bt=quota,
              cust_gddesc_extract_overdraft_bt=overdraft,
              cust_gddesc_exist_overdraft0=if_else(overdraft>0,0,1),
              cust_gddesc_exist_quota0=if_else(quota>0,0,1),
              cust_gddiff_minus_overdraftquota_bt_amt=minus(overdraft,quota),
              cust_gddiff_divide_overdraftquota_bt_amt=divide(overdraft,quota),
              cust_gddiff_difrate_overdraftquota_bt_amt=difrate(overdraft,quota)
    )
  names(var) <- str_replace(names(var),pattern = 'cust_',replacement = 'cust_jdbt_')
  return(var)
}

#-------------------------------------------------------#









###################################fdesc_bankcard_info.R##########################
# definition: list of main_bank_name.
main_bankname_list <- c('ccb','icbc','abc','cmb','citi','boc','ceb','post','bcom','gdb','pab','cmbc','spdb','cib','hxb','others')
names(main_bankname_list) <- c('ccb','icbc','abc','cmb','citi','boc','ceb','post','bcom','gdb','pab','cmbc','spdb','cib','hxb','others')

# definition: list of  major4_bank_name.
major4banks_list <- c('abc','boc','ccb','icbc')
names(major4banks_list) <- c('abc','boc','ccb','icbc')

# definition: list of card_type.
bankcard_type_list <- c("信用卡","储蓄卡")
names(bankcard_type_list) <- c("credit","saving")

# function: calc count for single filtered_value.
calc_toMatchName_cnt <- function(name, name_to_match, all_name_list){
  if(name_to_match=='all'){
    sum(if_else(!is.na(name),1,0))
  } else if(name_to_match=='others'){
    sum(if_else(!name%in%all_name_list, 1,0) )
  } else{
    sum(if_else(name == name_to_match,1,0))
  }
}

# function:  calc count of filtered_list.
calc_toMatchList_cnt <- function (name, list_to_match){
  sum(if_else(name%in%list_to_match,1,0))
}

# function: recognize whether name in to_match_list
if_existIn_toMatchList <-function (name, to_match_list){
  cnt <- sum(if_else(name%in%to_match_list, 1,0))
  if(cnt >0){ flag <- 1 }else {flag <- 0}
  return(flag)
}



desc_bankcardinfo<- function(jd_bankcard_rightjoin_appno) {
  #source('/home/datashare/crawl_data/代码归档/共用小函数整理.R')
  #source('/home/datashare/crawl_data/lq_code/utils_lq.R')

  jd_bankcard_raw <- jd_bankcard_rightjoin_appno %>%
    select(appl_no,custorm_id,login_name,card_id,bank_name,card_type,owner_name,phone)%>%
    filter(!is.na(custorm_id))
  # force "bank_name"  as  lower
  jd_bankcard_raw$bank_name <- tolower(jd_bankcard_raw$bank_name)

  # calc. count of each_filtered_bankname.
  exp_cmd <- paste0('cntbankname',c('all',main_bankname_list)," = calc_toMatchName_cnt(bank_name,'", c('all',main_bankname_list), "',main_bankname_list)", collapse = ',' )
  cmd_txt <- paste0("cust_fdesc_bankname_cntbankname <- jd_bankcard_raw%>%
                    select(appl_no,custorm_id,bank_name)%>%group_by(appl_no,custorm_id)%>%summarise(",exp_cmd,")")

  eval(parse(text=cmd_txt))

  # rename.
  names(cust_fdesc_bankname_cntbankname)[3:length(cust_fdesc_bankname_cntbankname)] <- paste0("cust_fdesc_n_bankname_",c('all',main_bankname_list))

  # calc cnt of  major4_bankname.
  cust_fdesc_n_bankname_major4banks <- jd_bankcard_raw%>%group_by(appl_no,custorm_id)%>%summarise(
    cust_fdesc_n_bankname_major4banks = calc_toMatchList_cnt(bank_name,major4banks_list)
  )

  cust_fdesc_bankname_cntbankname <- Reduce(f =full_join,list(cust_fdesc_bankname_cntbankname,cust_fdesc_n_bankname_major4banks))

  #calc. cnt_pct of bankname:
  input_1 <- names(cust_fdesc_bankname_cntbankname)[4:length(cust_fdesc_bankname_cntbankname)]
  input_2 <- names(cust_fdesc_bankname_cntbankname)[3]
  input_3 <- paste0('cust_fdiff_divide_',c(main_bankname_list,"major4banks"),'all_cntbankname')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('cust_fdesc_bankname_cntbankname %>% mutate(',str_exp,')')
  cust_fdesc_bankname_pctcntbankname <- eval(parse(text=text))

  # 1st merge:  cust_fdesc_bankname_pctcntbankname

  # whehter contains major4_bankname
  cust_fdesc_exsit_major4banks <- jd_bankcard_raw%>%group_by(appl_no,custorm_id)%>%summarise(
    cust_desc_exist_major4banks = if_existIn_toMatchList(bank_name,major4banks_list)
  )

  # 2nd merge: cust_fdesc_exsit_major4banks

  # 11-12:
  # calc. cnt of each cardtype.
  exp_cmd <- paste0('cntcardtype',c('all',names(bankcard_type_list))," = calc_toMatchName_cnt(card_type,'", c('all',bankcard_type_list), "',bankcard_type_list)", collapse = ',' )
  cmd_txt <- paste0("cust_fdesc_cardtype_cntcardtype <- jd_bankcard_raw%>%
                    select(appl_no,custorm_id,card_type)%>%group_by(appl_no,custorm_id)%>%summarise(",exp_cmd,")")

  eval(parse(text=cmd_txt))

  names(cust_fdesc_cardtype_cntcardtype)[3:length(cust_fdesc_cardtype_cntcardtype)] <- paste0("cust_fdesc_n_cardtype_",c('all',names(bankcard_type_list)))

  # calc. pct cnt of each cardtype.
  input_1 <- names(cust_fdesc_cardtype_cntcardtype)[4:length(cust_fdesc_cardtype_cntcardtype)]
  input_2 <- names(cust_fdesc_cardtype_cntcardtype)[3]
  input_3 <- paste0('cust_fdiff_divide_',names(bankcard_type_list),'all_cntcardtype')

  str_exp=paste0(input_3,'=',paste0('divide(',input_1,',',input_2,')') ,collapse = ',')
  text=paste0('cust_fdesc_cardtype_cntcardtype %>% mutate(',str_exp,')')
  cust_fdesc_banktype_pctcntcardtype <- eval(parse(text=text))

  # whehter exist one-single card_type:
  str_exp=paste0("cust_desc_exist_cardtype_",names(bankcard_type_list),' = if_else(cust_fdesc_n_cardtype_',names(bankcard_type_list),'>0, 1,0 )' , collapse = ',')
  text=paste0('cust_fdesc_banktype_pctcntcardtype %>% mutate(',str_exp,')')
  cust_fdesc_banktype_pctcntcardtype <- eval(parse(text=text))

  # 3rd merge: cust_fdesc_banktype_pctcntcardtype


  # --phone
  #1 多少个绑定的手机号
  #1 多少个绑定的不同的手机号 , 总的手机号个数/不同手机号个数

  # calc. count of bind_phone.
  #1226加了一个语句，filter(!is.na(phone))
  cust_desc_pctcntbindphone <- jd_bankcard_raw%>%
    select(appl_no,custorm_id,phone)%>%filter(!is.na(phone))%>%
    group_by(appl_no,custorm_id)%>%summarise(
      cust_desc_n_bindingphone = n(),
      cust_desc_nd_bindingphone = nd(phone),
      cust_diff_divide_nnd_bindphone = cust_desc_n_bindingphone / cust_desc_nd_bindingphone

    )

  # 4th merge: cust_desc_pctcntbindphone


  #### reduce
  reduce_list = list(
    cust_fdesc_bankname_pctcntbankname,
    cust_fdesc_exsit_major4banks,
    cust_fdesc_banktype_pctcntcardtype,
    cust_desc_pctcntbindphone

  )

  vars_bankcardinfo <- Reduce(full_join,reduce_list)

  # rename all the features. cust_  --> cust_all_
  #names(vars_bankcardinfo) <- str_replace(names(vars_bankcardinfo),pattern = 'cust_',replacement = 'cust_all_')

  names(vars_bankcardinfo) <- str_replace(names(vars_bankcardinfo),pattern = 'cust_',replacement = 'cust_jdbankcard_')
  return(vars_bankcardinfo)

}

###########################将补充的变量计算函数加在这，主要为需要和其他用户表关联##
compare_f=function(a,b){
  n=if_else(is.na(a)|is.na(b),0,if_else(a==b,1,0))
  return(n)
}

compute_var_cross_bankcardaddr=function(df1,df2){
  a=df1%>%select(appl_no,custorm_id,owner_name)%>%
    full_join(df2%>%select(appl_no,custorm_id,receiver))%>%
    mutate(owner_name_new=str_sub(owner_name,2,nchar(owner_name)),
           receiver_new=str_sub(receiver,2,nchar(receiver)),
           compare_bankname_receiver=compare_f(owner_name_new,receiver_new))%>%
    group_by(appl_no,custorm_id)%>%
    summarise(
      n_ownername_receiver=sum(compare_bankname_receiver),
      exist_ownername_receiver=if_else(n_ownername_receiver>0,1,0),
      divide_n_ownername_receiver=divide(n_ownername_receiver,n()))

  names(a)[3:length(a)]=paste0('cust_jdbankcard_desc_',names(a)[3:length(a)])
  return(a)
}

compute_var_cross_bankcardauth=function(df1,df2){
  a=df1%>%select(appl_no,custorm_id,phone)%>%rename(bankphone=phone)%>%
    full_join(df2%>%select(appl_no,custorm_id,phone))%>%
    mutate(compare_bankphone_phone=compare_f(bankphone,phone))%>%
    group_by(appl_no,custorm_id)%>%
    summarise(
      n_bankphone_authphone=sum(compare_bankphone_phone),
      exist_bankphone_authphone=if_else(n_bankphone_authphone>0,1,0),
      divide_n_bankphone_authphone=divide(n_bankphone_authphone,n()))

  names(a)[3:length(a)]=paste0('cust_jdbankcard_desc_',names(a)[3:length(a)])
  return(a)
}
#12 21修改 ，function中传入参数
compute_var_cross_bankcard=function(df_bankcard,df_receiveaddr,df_auth){
  a=compute_var_cross_bankcardaddr(df_bankcard,df_receiveaddr)
  b=compute_var_cross_bankcardauth(df_bankcard,df_auth)
  var=Reduce(full_join,list(a,b))
  return(var)
}

####封装上述函数
fdesc_bankcard_info=function(df_bankcard,df_receiveaddr,df_auth){
  df_bankcard=df_bankcard%>%filter(!is.na(custorm_id))
  var=desc_bankcardinfo(df_bankcard)
  var_cross=compute_var_cross_bankcard(df_bankcard,df_receiveaddr,df_auth)
  var=Reduce(full_join,list(var,var_cross))
  return(var)
}

#测试
#fdesc_bankcard_info(df_bankcard%>%head(0)) 48列

#---------------------------------------------------------------#













##############################fdesc_receiveaddr_info.R#########################
data_preparation_receivaddr_f=function(jd_receive_addr_rightjoin_appno){
  jd_receive_addr_rightjoin_appno=jd_receive_addr_rightjoin_appno%>%data.frame()
  jd_receivaddr <- jd_receive_addr_rightjoin_appno %>% filter(!is.na(rowkey)) %>%select(appl_no,custorm_id,region,phone,receiver,fix_phone,email)

  # 源数据处理，处理成可以和 字典表的标准数据关联的
  province_end <- ifelse(str_sub(jd_receivaddr[,'region'],1,2)%in%c('内蒙','黑龙','钓鱼') ,1,0)+2
  # 提取出省名，单独成列
  province_name <- str_sub(jd_receivaddr[,'region'],1,province_end)
  jd_receivaddr[,'prov_name'] <- province_name
  # 提取城市名，单独成列
  city_name <- str_sub(jd_receivaddr[,'region'],province_end+1,str_locate(jd_receivaddr[,'region'],'[市|区|州]')[,1])
  jd_receivaddr[,'city_name'] <- city_name
  #城市名称中 含有 ‘州’ 字的 补充 成‘州市’
  jd_receivaddr[,'city_name'] <- str_replace(jd_receivaddr[,'city_name'],'州','州市')

  jd_receivaddr[,'city_name'][jd_receivaddr[,'prov_name']=='北京'] <-'北京'
  jd_receivaddr[,'city_name'][jd_receivaddr[,'prov_name']=='上海'] <-'上海'
  jd_receivaddr[,'city_name'][jd_receivaddr[,'prov_name']=='重庆'] <-'重庆'
  jd_receivaddr[,'city_name'][jd_receivaddr[,'prov_name']=='天津'] <-'天津'

  jd_receivaddr_addcol <- jd_receivaddr
  #注意，此处的jd_receivaddr为data.frame格式
}

#输入为经过处理的data_preparation_receivaddr_f数据框

jdreceivaddr_vars <- function( jd_receivaddr_addcol) {
  jdreceivaddr_directvars <- jd_receivaddr_addcol%>%mutate(isna_fixphone= as.numeric(is.na(fix_phone)),
                                                           isna_email= as.numeric(is.na(email)),
                                                           isemail_qq = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]qq\\.')),
                                                           isemail_163 = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]163\\.')),
                                                           isemail_126 = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]126\\.')),
                                                           isemail_sina = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]sina\\.')),
                                                           isemail_139 = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]139\\.')),
                                                           isemail_hotmail = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]hotmail\\.')),
                                                           isemail_foxmail = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]foxmail\\.')),
                                                           isemail_189 = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]189\\.')),
                                                           isemail_sohu = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]sohu\\.')),
                                                           isemail_gmail = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]gmail\\.')),
                                                           isemail_yahoo = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]yahoo\\.')),
                                                           isemail_aliyun = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]aliyun\\.')),
                                                           isemail_yeah = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]yeah\\.')),
                                                           isemail_live = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]live\\.')),
                                                           isemail_outlook = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]outlook\\.')),
                                                           isemail_icloud = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@.]icloud\\.')),
                                                           isvipemail = as.numeric(str_detect(tolower(jd_receivaddr_addcol[,'email']),pattern = '[@]vip\\.')),
                                                           isemail_other=ifelse((isemail_qq==0)&(isemail_163==0)&(isemail_126==0)&(isemail_sina==0)&(isemail_139==0)&(isemail_hotmail==0)&(isemail_foxmail==0)&(isemail_189==0)&(isemail_sohu==0)&(isemail_gmail==0)&(isemail_yahoo==0)&(isemail_aliyun==0)&(isemail_yeah==0)&(isemail_live==0)&(isemail_outlook==0)&(isemail_icloud==0)&(isvipemail==0),1,0)
  ) %>%
    group_by(appl_no,custorm_id) %>%
    summarise(
      cust_n_address=n(),
      cust_nd_addrprov=nd(prov_name),
      cust_nd_addrcity=nd(city_name),
      cust_nd_receiver=nd(receiver),
      cust_nd_receiver=nd(phone),
      cust_n_nafixphone=sum(isna_fixphone,na.rm=T),
      cust_rate_nafixphone=cust_n_nafixphone/cust_n_address,
      cust_n_naemail=sum(isna_email,na.rm=T),
      cust_rate_naemail=cust_n_naemail/cust_n_address,
      cust_n_email_qq     =sum(isemail_qq      ,na.rm=T),
      cust_n_email_163    =sum(isemail_163     ,na.rm=T),
      cust_n_email_126    =sum(isemail_126     ,na.rm=T),
      cust_n_email_sina   =sum(isemail_sina    ,na.rm=T),
      cust_n_email_139    =sum(isemail_139     ,na.rm=T),
      cust_n_email_hotmail=sum(isemail_hotmail ,na.rm=T),
      cust_n_email_foxmail=sum(isemail_foxmail ,na.rm=T),
      cust_n_email_189    =sum(isemail_189     ,na.rm=T),
      cust_n_email_sohu   =sum(isemail_sohu    ,na.rm=T),
      cust_n_email_gmail  =sum(isemail_gmail   ,na.rm=T),
      cust_n_email_yahoo  =sum(isemail_yahoo   ,na.rm=T),
      cust_n_email_aliyun =sum(isemail_aliyun  ,na.rm=T),
      cust_n_email_yeah   =sum(isemail_yeah    ,na.rm=T),
      cust_n_email_live   =sum(isemail_live    ,na.rm=T),
      cust_n_email_outlook=sum(isemail_outlook ,na.rm=T),
      cust_n_email_icloud =sum(isemail_icloud  ,na.rm=T),
      cust_n_email_vip    =sum(isvipemail      ,na.rm=T),
      cust_n_email_other  =sum(isemail_other   ,na.rm=T)
    )

  names(jdreceivaddr_directvars) <- str_replace(names(jdreceivaddr_directvars),pattern = 'cust_',replacement = 'cust_desc_')


  jd_receivaddr_prov_cntaddr <-  jd_receivaddr_addcol%>%group_by(appl_no,custorm_id,prov_name)%>%
    summarise( num_addr=n() )%>%group_by(appl_no,custorm_id)%>%summarise_each( funs(entropy,normentropy),num_addr )%>%
    rename(cust_gdesc_entropy_prov_cntaddr=entropy,cust_gdesc_normentropy_prov_cntaddr=normentropy)

  jd_receivaddr_city_cntaddr <-  jd_receivaddr_addcol%>%group_by(appl_no,custorm_id,city_name)%>%
    summarise( num_addr=n() )%>%group_by(appl_no,custorm_id)%>%summarise_each( funs(entropy,normentropy),num_addr )%>%
    rename(cust_gdesc_entropy_city_cntaddr=entropy,cust_gdesc_normentropy_city_cntaddr=normentropy)


  jd_receivaddr_phone_cntaddr <-  jd_receivaddr_addcol%>%group_by(appl_no,custorm_id,phone)%>%
    summarise( num_addr=n() )%>%group_by(appl_no,custorm_id)%>%summarise_each( funs(entropy,normentropy),num_addr )%>%
    rename(cust_gdesc_entropy_phone_cntaddr=entropy,cust_gdesc_normentropy_phone_cntaddr=normentropy)



  ls <-  list(jdreceivaddr_directvars,jd_receivaddr_prov_cntaddr,jd_receivaddr_city_cntaddr,jd_receivaddr_phone_cntaddr)
  jdreceivaddr_vars <-  Reduce(full_join,ls)

  names(jdreceivaddr_vars) <- str_replace(names(jdreceivaddr_vars),pattern = 'cust_',replacement = 'cust_jdreceivaddr_')
  jdreceivaddr_vars
}


fdesc_receiveaddr_info=function(df){
  var=df%>%data_preparation_receivaddr_f()%>%jdreceivaddr_vars()%>%tbl_df()
  return(var)
}

#-----------------------------------------------------------#


#######################################
#以上部分为计算jd_five_table部分的函数#
#######################################















#####################################################################################
#调用函数#
#################################################################################

#封装的eval函数
f_eval=function(x){
  eval(parse(text=x))
}

##使用eval的方式进行封装，最大的缺点：
#其对mclapply封装时，其最大的缺点，只会从内存里面找对象，必须加装到内存中的对象其才能
#找到并执行。所以使用 <<- 符合，开辟内存空间。
#但是，本人亲自试验发现，对于传入的数据集（需要对数据集并行，且以形参的形式传入），其找不到。
#所以，需要将需要执行的数据集提前加载到内存中，供其使用。
#cannot change value of locked binding for 'df'

#并行计算order_product部分的变量
var_calculate_orderproduct=function(df){
  df_<<-df
  exp_order_product_xf_1<<-list(
    'fdesc_hourbin_amtaorder.02w <- fdesc_hourbin_amtaorder(df_,14)',
    'fdesc_hourbin_amtaorder.01m <- fdesc_hourbin_amtaorder(df_,30)',
    'fdesc_hourbin_amtaorder.03m <- fdesc_hourbin_amtaorder(df_,90)',
    'fdesc_hourbin_amtaorder.06m <- fdesc_hourbin_amtaorder(df_,180)',
    'fdesc_hourbin_amtaorder.01y <- fdesc_hourbin_amtaorder(df_,360)',
    'fdesc_hourbin_amtaorder.all <- fdesc_hourbin_amtaorder(df_,Inf)',

    'tsdesc_amtorderdiff.02w <- tsdesc_amtorderdiff(df_,14)',
    'tsdesc_amtorderdiff.01m <- tsdesc_amtorderdiff(df_,30)',
    'tsdesc_amtorderdiff.03m <- tsdesc_amtorderdiff(df_,90)',
    'tsdesc_amtorderdiff.06m <- tsdesc_amtorderdiff(df_,180)',
    'tsdesc_amtorderdiff.01y <- tsdesc_amtorderdiff(df_,360)',
    'tsdesc_amtorderdiff.all <- tsdesc_amtorderdiff(df_,Inf)'

  )

  exp_order_product_xf_2<<-list(
    'gdesc_order_product.02w <- gdesc_order_product(df_,14)',
    'gdesc_order_product.01m <- gdesc_order_product(df_,30)',
    'gdesc_order_product.03m <- gdesc_order_product(df_,90)'

  )

  exp_order_product_xf_3<<-list(
    'gdesc_order_product.06m <- gdesc_order_product(df_,180)',
    'gdesc_order_product.01y <- gdesc_order_product(df_,360)',
    'gdesc_order_product.all <- gdesc_order_product(df_,Inf)'
  )
  #mclapply(exp_order_product_xf_2,f_time,mc.cores =1)

  exp_code_lyk1<<-list(
    'fdesc_hourbin_cntorder_2w=fdesc_hourbin_cntorder(df_,time_gap=14)',
    'fdesc_hourbin_cntorder_1m=fdesc_hourbin_cntorder(df_,time_gap=30)',
    'fdesc_hourbin_cntorder_3m=fdesc_hourbin_cntorder(df_,time_gap=90)',
    'fdesc_hourbin_cntorder_6m=fdesc_hourbin_cntorder(df_,time_gap=180)',
    'fdesc_hourbin_cntorder_12m=fdesc_hourbin_cntorder(df_,time_gap=360)',
    'fdesc_hourbin_cntorder_all=fdesc_hourbin_cntorder(df_,time_gap=Inf)'
  )

  exp_code_lyk2<<-list(
    'fdesc_sts_cntorder_2w=fdesc_sts_cntorder(df_,time_gap=14)',
    'fdesc_sts_cntorder_1m=fdesc_sts_cntorder(df_,time_gap=30)',
    'fdesc_sts_cntorder_3m=fdesc_sts_cntorder(df_,time_gap=90)',
    'fdesc_sts_cntorder_6m=fdesc_sts_cntorder(df_,time_gap=180)',
    'fdesc_sts_cntorder_12m=fdesc_sts_cntorder(df_,time_gap=360)',
    'fdesc_sts_cntorder_all=fdesc_sts_cntorder(df_,time_gap=Inf)'
  )

  exp_code2<<-list(
    'var1=gddif_hourbin_cntorder(Reduce(full_join,lapply(exp_code_lyk1,f_eval)))',
    'var2=gddif_sts_cntorder(Reduce(full_join,lapply(exp_code_lyk2,f_eval)))'
  )

  exp_code_lyk3<<-list(
    'fdesc_pay_cntorder_2w=fdesc_pay_cntorder(df_,time_gap=14)',
    'fdesc_pay_cntorder_1m=fdesc_pay_cntorder(df_,time_gap=30)',
    'fdesc_pay_cntorder_3m=fdesc_pay_cntorder(df_,time_gap=90)',
    'fdesc_pay_cntorder_6m=fdesc_pay_cntorder(df_,time_gap=180)',
    'fdesc_pay_cntorder_12m=fdesc_pay_cntorder(df_,time_gap=360)',
    'fdesc_pay_cntorder_all=fdesc_pay_cntorder(df_,time_gap=Inf)',

    'fdesc_accname_cntorder_2w=fdesc_accname_cntorder(df_,time_gap=14)',
    'fdesc_accname_cntorder_1m=fdesc_accname_cntorder(df_,time_gap=30)',
    'fdesc_accname_cntorder_3m=fdesc_accname_cntorder(df_,time_gap=90)',
    'fdesc_accname_cntorder_6m=fdesc_accname_cntorder(df_,time_gap=180)',
    'fdesc_accname_cntorder_12m=fdesc_accname_cntorder(df_,time_gap=360)',
    'fdesc_accname_cntorder_all=fdesc_accname_cntorder(df_,time_gap=Inf)',

    'fdesc_invoicehead_cntorder_2w=fdesc_invoicehead_cntorder(df_,time_gap=14)',
    'fdesc_invoicehead_cntorder_1m=fdesc_invoicehead_cntorder(df_,time_gap=30)',
    'fdesc_invoicehead_cntorder_3m=fdesc_invoicehead_cntorder(df_,time_gap=90)',
    'fdesc_invoicehead_cntorder_6m=fdesc_invoicehead_cntorder(df_,time_gap=180)',
    'fdesc_invoicehead_cntorder_12m=fdesc_invoicehead_cntorder(df_,time_gap=360)',
    'fdesc_invoicehead_cntorder_all=fdesc_invoicehead_cntorder(df_,time_gap=Inf)',

    'fdesc_invoicecontent_cntorder_2w=fdesc_invoicecontent_cntorder(df_,time_gap=14)',
    'fdesc_invoicecontent_cntorder_1m=fdesc_invoicecontent_cntorder(df_,time_gap=30)',
    'fdesc_invoicecontent_cntorder_3m=fdesc_invoicecontent_cntorder(df_,time_gap=90)',
    'fdesc_invoicecontent_cntorder_6m=fdesc_invoicecontent_cntorder(df_,time_gap=180)',
    'fdesc_invoicecontent_cntorder_12m=fdesc_invoicecontent_cntorder(df_,time_gap=360)',
    'fdesc_invoicecontent_cntorder_all=fdesc_invoicecontent_cntorder(df_,time_gap=Inf)'

  )

  exp_code_lyk4<<-list(
    'fdesc_pay_amtorder_2w=fdesc_pay_amtorder(df_,time_gap=14)',
    'fdesc_pay_amtorder_1m=fdesc_pay_amtorder(df_,time_gap=30)',
    'fdesc_pay_amtorder_3m=fdesc_pay_amtorder(df_,time_gap=90)',
    'fdesc_pay_amtorder_6m=fdesc_pay_amtorder(df_,time_gap=180)',
    'fdesc_pay_amtorder_12m=fdesc_pay_amtorder(df_,time_gap=360)',
    'fdesc_pay_amtorder_all=fdesc_pay_amtorder(df_,time_gap=Inf)',

    'fdesc_accname_amtorder_2w=fdesc_accname_amtorder(df_,time_gap=14)',
    'fdesc_accname_amtorder_1m=fdesc_accname_amtorder(df_,time_gap=30)',
    'fdesc_accname_amtorder_3m=fdesc_accname_amtorder(df_,time_gap=90)',
    'fdesc_accname_amtorder_6m=fdesc_accname_amtorder(df_,time_gap=180)',
    'fdesc_accname_amtorder_12m=fdesc_accname_amtorder(df_,time_gap=360)',
    'fdesc_accname_amtorder_all=fdesc_accname_amtorder(df_,time_gap=Inf)',

    'fdesc_invoicehead_amtorder_2w=fdesc_invoicehead_amtorder(df_,time_gap=14)',
    'fdesc_invoicehead_amtorder_1m=fdesc_invoicehead_amtorder(df_,time_gap=30)',
    'fdesc_invoicehead_amtorder_3m=fdesc_invoicehead_amtorder(df_,time_gap=90)',
    'fdesc_invoicehead_amtorder_6m=fdesc_invoicehead_amtorder(df_,time_gap=180)',
    'fdesc_invoicehead_amtorder_12m=fdesc_invoicehead_amtorder(df_,time_gap=360)',
    'fdesc_invoicehead_amtorder_all=fdesc_invoicehead_amtorder(df_,time_gap=Inf)',

    'fdesc_invoicecontent_amtorder_2w=fdesc_invoicecontent_amtorder(df_,time_gap=14)',
    'fdesc_invoicecontent_amtorder_1m=fdesc_invoicecontent_amtorder(df_,time_gap=30)',
    'fdesc_invoicecontent_amtorder_3m=fdesc_invoicecontent_amtorder(df_,time_gap=90)',
    'fdesc_invoicecontent_amtorder_6m=fdesc_invoicecontent_amtorder(df_,time_gap=180)',
    'fdesc_invoicecontent_amtorder_12m=fdesc_invoicecontent_amtorder(df_,time_gap=360)',
    'fdesc_invoicecontent_amtorder_all=fdesc_invoicecontent_amtorder(df_,time_gap=Inf)',

    'fdesc_prdname_prdcategory_2w=fdesc_prdname_prdcategory(df_,time_gap=14)',
    'fdesc_prdname_prdcategory_1m=fdesc_prdname_prdcategory(df_,time_gap=30)',
    'fdesc_prdname_prdcategory_3m=fdesc_prdname_prdcategory(df_,time_gap=90)',
    'fdesc_prdname_prdcategory_6m=fdesc_prdname_prdcategory(df_,time_gap=180)',
    'fdesc_prdname_prdcategory_12m=fdesc_prdname_prdcategory(df_,time_gap=360)',
    'fdesc_prdname_prdcategory_all=fdesc_prdname_prdcategory(df_,time_gap=Inf)'
  )

  exp_code_lyk5<<-list(
    'gddesc_jd618=gddesc_jd618(df_)',
    'gddesc_jd1111=gddesc_jd1111(df_)'

  )

  exp_code_lyk6<<-list(

    'gddesc_springfestival=gddesc_springfestival(df_)',
    'gddesc_loantime_now=gddesc_loantime_now(df_)'
  )
  #mclapply(exp_code_lyk3,f_time,mc.cores =1)

  exp_order_product<<-list(
    'Reduce(full_join,mclapply(exp_order_product_xf_1,f_eval,mc.cores =1))',
    'Reduce(full_join,mclapply(exp_order_product_xf_2,f_eval,mc.cores =3))[,c(-1,-2)]',
    'Reduce(full_join,mclapply(exp_order_product_xf_3,f_eval,mc.cores =3))[,c(-1,-2)]',
    'Reduce(full_join,mclapply(exp_code2,f_eval,mc.cores =2))[,c(-1,-2)]',
    'Reduce(full_join,mclapply(exp_code_lyk3,f_eval,mc.cores =1))[,c(-1,-2)]',
    'Reduce(full_join,mclapply(exp_code_lyk4,f_eval,mc.cores =2))[,c(-1,-2)]',
    'Reduce(full_join,mclapply(exp_code_lyk5,f_eval,mc.cores =3))[,c(-1,-2)]',
    'Reduce(full_join,mclapply(exp_code_lyk6,f_eval,mc.cores =1))[,c(-1,-2)]'
  )

  # exp_order_product<<-list(
  #   'Reduce(full_join,mclapply(exp_order_product_xf_1,f_eval,mc.cores =1))',
  #   'Reduce(full_join,mclapply(exp_order_product_xf_2,f_eval,mc.cores =1))[,c(-1,-2)]',
  #   'Reduce(full_join,mclapply(exp_order_product_xf_3,f_eval,mc.cores =1))[,c(-1,-2)]',
  #   'Reduce(full_join,mclapply(exp_code2,f_eval,mc.cores =1))[,c(-1,-2)]',
  #   'Reduce(full_join,mclapply(exp_code_lyk3,f_eval,mc.cores =1))[,c(-1,-2)]',
  #   'Reduce(full_join,mclapply(exp_code_lyk4,f_eval,mc.cores =1))[,c(-1,-2)]',
  #   'Reduce(full_join,mclapply(exp_code_lyk5,f_eval,mc.cores =1))[,c(-1,-2)]',
  #   'Reduce(full_join,mclapply(exp_code_lyk6,f_eval,mc.cores =1))[,c(-1,-2)]'
  # )
  #VARs=Reduce(full_join,mclapply(exp_order_product,f_eval,mc.cores =3))
  #此处，用cbind替换full_join，能节约大约3秒
  #但是，为什么不把所有的full_join都替换了呢？
  #原因：会造成大量重复的列（c("appl_no", "custorm_id")），
  #去除重复列所消耗的时间和cbind节约出的时间相差毫秒级,且增加了代码复杂度。
  #再者，当cbind会快于full_join的前提条件是，join的两个数据库列数不对等，n1>10*n2 & n2>500
  #1219发现的问题，使用cbind时出现的问题，一个0行的数据框不能和1行的bind。而前期有大量的0行。

  var=mclapply(exp_order_product,f_eval,mc.cores =8)
  #var=mclapply(exp_order_product,f_eval,mc.cores =1)

  VARs=Reduce(bind_cols,lapply(var,f_transform))
  #VARs=mclapply(exp_order_product,f_time,mc.cores =1)
  return(VARs)
}

#测试
# df=df_order_product%>%filter(appl_no=='20160731100005497078')
# var_calculate_orderproduct(df_order_product%>%filter(appl_no=='20160731100005497078'))
# var_calculate_orderproduct(df_order_product%>%head(1),mc_core=2)

# f_time=function(x){
#   t1=Sys.time()
#   f_eval(x)
#   Sys.time()-t1
# }

#此函数的作用是，将0行的数据装换为1行
f_transform=function(x){
  if(dim(x)[1]==0){
    x=lapply(x,function(x){x=NA})%>%tbl_df()
    return(x)
  }else{
    return(x)
  }
}

#####计算京东五张表相关的变量
var_calculate_fivetable=function(){
  exp_calculate_fivetable<<-list(
    'fdesc_auth_info(df_auth)',
    'fdesc_user_info(df_user)',
    'fdesc_bt_info(df_bt)',
    'fdesc_bankcard_info(df_bankcard)',
    'fdesc_receiveaddr_info(df_receiveaddr)'
  )

  var=Reduce(full_join,mclapply(exp_calculate_fivetable,f_eval))

  return(var)
}










###################################读取接口中的数据##########

#####################
# query_base_interface=function(query_url,param){
#   raw_datajson=postForm(query_url,
#                         customId=param$customId,
#                         applNo=param$applNo,
#                         crawlType=param$crawlType,
#                         tableName=param$tableName,
#                         fields=param$fields,
#                         style = "POST"
#   )[1]
#
#   ls <- lapply(fromJSON(raw_datajson),tbl_df)
#
#   data=Reduce(bind_rows,ls)
#
#   return(data)
# }

#1229设置了连接超时机制
query_base_interface=function(query_url,param){

  tryCatch({

    raw_datajson=postForm(query_url,.opts = curlOptions(connecttimeout = 3),
                          customId=param$customId,
                          applNo=param$applNo,
                          crawlType=param$crawlType,
                          tableName=param$tableName,
                          fields=param$fields,
                          style = "POST"
    )[1]

    ls <- lapply(fromJSON(raw_datajson),tbl_df)

    data=Reduce(bind_rows,ls)

    return(data)

  },
  error = function(e){
    # return(e)
    #return(toJSON(data.frame(code=701,message=paste0(e),data="null")))
    return(NULL)
  })

}


# save_base_interface=function(save_url,param){
#   res_message=postForm(save_url,
#                         customId=param$customId,
#                         applNo=param$applNo,
#                         crawlType=param$crawlType,
#                         tableName=param$tableName,
#                         varData=param$varData,
#                         style = "POST"
#   )
#
#   return(res_message)
# }

#1229设置了连接超时机制
save_base_interface=function(save_url,param){
  tryCatch({
  res_message=postForm(save_url,.opts = curlOptions(connecttimeout = 3),
                       customId=param$customId,
                       applNo=param$applNo,
                       crawlType=param$crawlType,
                       tableName=param$tableName,
                       varData=param$varData,
                       style = "POST"
  )

  return(res_message)
  },
  error = function(e){
    # return(e)
    #return(toJSON(data.frame(code=701,message=paste0(e),data="null")))
    return(NULL)
  })
}



#####
#对数据集中的key做唯一性校验（appl_no,custorm_id）,确保唯一，且出现空值（""）时，对其进行填充。
#12月26号，和小峰讨论后发现，存在""的情况，需要将其处理为NA.同时，‘NA’也处理为NA。
key_check=function(df,value1,value2){
  #将"","NA"处理为NA
  if(is.null(df)){return(NULL)}
  df[df=="NA"]=NA
  df[df==""]=NA
  #校验key的唯一性和保值性（值存在）
  flag1=(length(unique(df$appl_no))==1 & length(unique(df$custorm_id))==1)
  flag2=(NA %in% df$appl_no | NA %in% df$custorm_id)
  if(flag1){
    if(flag2){
      df$appl_no=value1
      df$custorm_id=value2
    }
    return(df)
  }else{
    return("message:appl_no and custorm_id is not unique")
  }

}



#####################保存失败，多次保存##########
check_save=function(mess){
  #判断是否保存成功，返回的x是一个list,值为true or false
  x=lapply(mess,function(x){x=="{\"code\":200,\"message\":\"请求处理成功\",\"data\":null}"})
  flag=x%>%unlist()%>%all()
  n=0
  while(n<3){
    #所有的都保存成功，跳出。没有成功，继续执行一次存储语句，且最多保存5次。
    if(flag){
      break
    }else{
      f_eval("mclapply(save_list,f_eval,mc.cores = 3)")
      n=n+1}

  }
}
