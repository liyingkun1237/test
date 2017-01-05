
#param为智锋调用R包传入的三个参数，以key value的形式
msDpvAF007Api = function(param){

  #############
  #传入参数的校验:参数名，参数个数，参数类型，参数非空。
  #############
  if(length(param)!=3){
    return(toJSON(data.frame(code=701,message="The number of the input parameters is not equal to 3,Correct parameter names are \"customId\",\"applNo\",\"crawlType\"",data="null")))

  }else if(!all(names(param) %in% c("applNo","customId","crawlType"))){ #对参数的顺序没有要求 20170103改
    # else if(any(names(param)!=c("customId","applNo","crawlType"))){
    return(toJSON(data.frame(code=701,message="Input parameter names error,Correct parameter names are \"customId\",\"applNo\",\"crawlType\"",data="null")))

  }else if(! lapply(param,is.character)%>%unlist()%>%all()){
    return(toJSON(data.frame(code=701,message="The input parameter type must be character",data="null")))

  }else if(lapply(param,function(x){x==""|x=="NA"})%>%unlist()%>%any()){
    return(toJSON(data.frame(code=701,message="The value of the incoming parameter cannot be\"\" or \"NA\".",data="null")))

  }else{

    param<<-param

    #使用接口查询原始数据
    path='http://datavariable.msxf.lodev'
    query_url<<-paste0(path,'/dataDpt/queryCrawlData')

    orderprod_fields=c("prd_name","phone","product_id","custorm_id","homefurniture",
                       "invoice_type", "invoice_content" ,"colour" ,"gender" ,
                       "quantity", "financial","motherbabytoys","type_pay", "shoesbagsjewelry" ,
                       "discount" , "sportsoutdoor","foodswine", "appl_no","email",
                       "invoice_head" ,"travelrecharge","booksvideo" ,"clothing", "appl_sbm_tm",
                       "name_rec" ,"computeroffice" , "medicinehealth" ,"specialcharacter","time_order",
                       "add_rec", "phonedigital","makeupsuppliespet","no_order" ,"name_acc" ,
                       "domesticappliance","sts_order","order_time_int" ,"amt_order", "no_gid",
                       "automotiveproducts" )

    param_jd_orderprod<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='jd_orderprod',
      fields=orderprod_fields
    )
    # df_order_product=query_base_interface(query_url,param_jd_orderprod)
    # df_order_product=key_check(df_order_product,param$applNo,param$customId)


    auth_fields=c("phone","rowkey","idcard_end","custorm_id",
                  "phone_start", "idcard_start","id_card", "phone_end",
                  "auth_time" ,"appl_no", "real_name", "login_name",
                  "appl_sbm_tm_date", "finan_serv", "channel","appl_sbm_tm")
    param_jd_auth<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='jd_auth',
      fields=auth_fields
    )
    #加[1,]的原因，谦姐1221号，为容错机制设计
    # df_auth=query_base_interface(query_url,param_jd_auth)[1,]
    # df_auth=key_check(df_auth,param$applNo,param$customId)


    user_fields=c("web_login_name","birthday","sex" ,"rowkey","custorm_id",
                  "nickname","merriage" ,"hobby","wechat_bound","id_card",
                  "qq_bound","income_max", "user_name", "income_min",
                  "degree" ,"appl_no","email" ,"income", "real_name" ,
                  "login_name", "appl_sbm_tm_date","appl_sbm_tm",
                  "account_grade","industry","account_type" )
    param_jd_user<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='jd_user',
      fields=user_fields
    )
    #加[1,]的原因，谦姐1221号，为容错机制设计
    # df_user=query_base_interface(query_url,param_jd_user)[1,]
    # df_user=key_check(df_user,param$applNo,param$customId)


    bt_fields=c("quota","overdraft","rowkey","appl_no","custorm_id",
                "login_name","appl_sbm_tm_date","appl_sbm_tm", "credit_score")
    param_jd_bt<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='jd_bt',
      fields=bt_fields
    )
    #加[1,]的原因，谦姐1221号，为容错机制设计
    # df_bt=query_base_interface(query_url,param_jd_bt)[1,]
    # df_bt=key_check(df_bt,param$applNo,param$customId)


    bankcard_fields=c("phone","bank_name","rowkey","custorm_id" ,
                      "owner_name" ,"phone_start","tail_num", "phone_end",
                      "card_id","appl_no" ,"card_type","login_name",
                      "appl_sbm_tm_date","appl_sbm_tm")
    param_jd_bankcard<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='jd_bankcard',
      fields=bankcard_fields
    )
    # df_bankcard=query_base_interface(query_url,param_jd_bankcard)
    # df_bankcard=key_check(df_bankcard,param$applNo,param$customId)


    addr_fields=c("region","city_name","phone", "rowkey",
                  "custorm_id","receiver","phone_start","email",
                  "prov_name", "addr" ,"addr_id" , "phone_end" ,
                  "appl_no","login_name" ,"appl_sbm_tm_date", "appl_sbm_tm",
                  "fix_phone")
    param_jd_addr<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='jd_addr',
      fields=addr_fields
    )
    # df_receiveaddr=query_base_interface(query_url,param_jd_addr)
    # df_receiveaddr=key_check(df_receiveaddr,param$applNo,param$customId)

    ########并行的读取数据并对读入的数据进行校验
    read_list <<- list(
      df_order_product='key_check(query_base_interface(query_url,param_jd_orderprod),param$applNo,param$customId)',
      df_auth='key_check(query_base_interface(query_url,param_jd_auth)[1,],param$applNo,param$customId)',
      df_user='key_check(query_base_interface(query_url,param_jd_user)[1,],param$applNo,param$customId)',
      df_bt='key_check(query_base_interface(query_url,param_jd_bt)[1,],param$applNo,param$customId)',
      df_bankcard='key_check(query_base_interface(query_url,param_jd_bankcard),param$applNo,param$customId)',
      df_receiveaddr='key_check(query_base_interface(query_url,param_jd_addr),param$applNo,param$customId)'
    )

    #测试网络连接失败导致的数据读取失败
    # read_list <<- list(
    #   df_order_product='key_check(query_base_interface(query_url,param_jd_orderprod),param$applNo,param$customId)',
    #   df_auth='key_check(query_base_interface(query_url,param_jd_auth)[1,],param$applNo,param$customId)',
    #   df_user='key_check(query_base_interface(query_url,param_jd_user)[1,],param$applNo,param$customId)',
    #   df_bt='key_check(query_base_interface(query_url,param_jd_bt)[1,],param$applNo,param$customId)',
    #   df_bankcard='key_check(query_base_interface(query_url,param_jd_bankcard),param$applNo,param$customId)',
    #   df_receiveaddr='key_check(query_base_interface("http://www.google.com",param_jd_addr),param$applNo,param$customId)'
    # )


    data_list=mclapply(read_list,f_eval,mc.cores = 3)


    if(any(unlist(lapply(data_list, is.null)))){
      return(toJSON(data.frame(code=701,message="Error reading data (Connection timed out after 3000 milliseconds or data parsing faild)",data="null")))
    }


    df_order_product=data_list$df_order_product
    df_auth=data_list$df_auth
    df_user=data_list$df_user
    df_bt=data_list$df_bt
    df_bankcard=data_list$df_bankcard
    df_receiveaddr=data_list$df_receiveaddr



    ##########读取进来的数据进行预处理
    df_order_product$amt_order=as.numeric(df_order_product$amt_order)
    df_order_product$appl_sbm_tm=as.POSIXct(df_order_product$appl_sbm_tm,format="%Y-%m-%d %H:%M:%S")
    df_order_product$time_order=as.POSIXct(df_order_product$time_order,format="%Y-%m-%d %H:%M:%S")
    df_order_product$order_time_int <- as.numeric(df_order_product$order_time_int)
    df_order_product$specialcharacter=as.numeric(df_order_product$specialcharacter)

    #这些错误已交由智锋处理掉了
    #df_order_product=df_order_product%>%rename(appl_no=app_no)
    # df_auth = df_auth%>%rename(appl_no=app_no)
    # df_auth$appl_sbm_tm = df_auth$appl_sbm_tm_date
    #df_user=df_user%>%rename(appl_no=app_no)
    #df_bt=df_bt%>%rename(appl_no=app_no)
    #df_bankcard = df_bankcard%>%rename(appl_no=app_no)
    #df_receiveaddr = df_receiveaddr%>%rename(appl_no=app_no)
    #df_receiveaddr = df_receiveaddr%>%rename(email=email_addr)

    ##########################################################

    result=list(
      #var_crawl_ecomm_jd_order_product表
      var_crawl_ecomm_jd_order_product=var_calculate_orderproduct(df_order_product),

      #var_crawl_ecomm_jd_auth
      var_crawl_ecomm_jd_auth=fdesc_auth_info(df_auth),

      #var_crawl_ecomm_jd_user
      var_crawl_ecomm_jd_user=fdesc_user_info(df_user),

      #var_crawl_ecomm_jd_bt
      var_crawl_ecomm_jd_bt=fdesc_bt_info(df_bt),

      #var_crawl_ecomm_jd_bankcard
      var_crawl_ecomm_jd_bankcard=fdesc_bankcard_info(df_bankcard,df_receiveaddr,df_auth),

      #var_crawl_ecomm_jd_receiveaddr
      var_crawl_ecomm_jd_receiveaddr=fdesc_receiveaddr_info(df_receiveaddr)
    )





    ##########################存储计算好的变量###############
    path='http://datavariable.msxf.lodev'
    save_url<<-paste0(path,'/dataDpt/saveVarData')

    param_save_var_jd_orderprod<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='var_jd_orderprod',
      varData=toJSON(result$var_crawl_ecomm_jd_order_product[,c(-1,-2)])
    )

    #save_var_jd_orderprod_mess=save_base_interface(save_url,param_save_var_jd_orderprod)


    param_save_var_jd_auth<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='var_jd_auth',
      varData=toJSON(result$var_crawl_ecomm_jd_auth[,c(-1,-2)])
    )


    #save_var_jd_auth_mess=save_base_interface(save_url,param_save_var_jd_auth)


    param_save_var_jd_user<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='var_jd_user',
      varData=toJSON(result$var_crawl_ecomm_jd_user[,c(-1,-2)])
    )


    #save_var_jd_user_mess=save_base_interface(save_url,param_save_var_jd_user)


    param_save_var_jd_bt<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='var_jd_bt',
      varData=toJSON(result$var_crawl_ecomm_jd_bt[,c(-1,-2)])
    )


    #save_var_jd_bt_mess=save_base_interface(save_url,param_save_var_jd_bt)


    param_save_var_jd_bankcard<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='var_jd_bankcard',
      varData=toJSON(result$var_crawl_ecomm_jd_bankcard[,c(-1,-2)])
    )


    #save_var_jd_bankcard_mess=save_base_interface(save_url,param_save_var_jd_bankcard)


    param_save_var_jd_addr<<-list(
      customId=param$customId,
      applNo=param$applNo,
      crawlType=param$crawlType,
      tableName='var_jd_addr',
      varData=toJSON(result$var_crawl_ecomm_jd_receiveaddr[,c(-1,-2)])
      #测没有存取成功，重复存取
      #vardata=toJSON(result$var_crawl_ecomm_jd_receiveaddr[,c(-1,-2)])
    )


    #save_var_jd_addr_mess=save_base_interface(save_url,param_save_var_jd_addr)

    #########并行的存变量
    save_list <<-list(
      save_var_jd_orderprod_mess='save_base_interface(save_url,param_save_var_jd_orderprod)',
      save_var_jd_auth_mess='save_base_interface(save_url,param_save_var_jd_auth)',
      save_var_jd_user_mess='save_base_interface(save_url,param_save_var_jd_user)',
      save_var_jd_bt_mess='save_base_interface(save_url,param_save_var_jd_bt)',
      save_var_jd_bankcard_mess='save_base_interface(save_url,param_save_var_jd_bankcard)',
      save_var_jd_addr_mess='save_base_interface(save_url,param_save_var_jd_addr)'
    )

    #测试保存不成功

    # save_list <<-list(
    #   save_var_jd_orderprod_mess='save_base_interface(save_url,param_save_var_jd_orderprod)',
    #   save_var_jd_auth_mess='save_base_interface(save_url,param_save_var_jd_auth)',
    #   save_var_jd_user_mess='save_base_interface(save_url,param_save_var_jd_user)',
    #   save_var_jd_bt_mess='save_base_interface(save_url,param_save_var_jd_bt)',
    #   save_var_jd_bankcard_mess='save_base_interface(save_url,param_save_var_jd_bankcard)',
    #   save_var_jd_addr_mess='save_base_interface("http://www.google.com",param_save_var_jd_addr)'
    # )

    message_save=mclapply(save_list,f_eval,mc.cores = 3)
    #没有保存成功，会继续重复保存，且最多重复次数小于5次
    #判断是否保存成功
    #"{\"code\":200,\"message\":\"请求处理成功\",\"data\":null}"
    check_save(message_save)

    if(any(unlist(lapply(message_save, is.null)))){
      return(toJSON(data.frame(code=701,message="Error save data (Connection timed out after 3000 milliseconds )",data="null")))
    }

    #print(message_save)
    #return(message_save)

    #返回计算好的变量
    #return(result)
    return(toJSON(data.frame(code=200,message="Variable calculation successful and save completed",data="null")))
  }


}





