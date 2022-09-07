# BigQueryと接続
    bigquery = self.connectBigQuery

    bigquery.query "DELETE FROM `wacul-databeat.wacul_ad_test.cv_test_label` WHERE 1=1"

    (0..4).each do |index|
      serviceData = bigquery.query <<~SQL
                                     SELECT
                                       DISTINCT PromotionId,
                                       PromotionName,
                                       ServiceId,
                                       ServiceAccountId
                                     FROM
                                       `wacul-databeat.databeat.databeat_attribute` databeat                           
                                     ORDER BY PromotionName
                                     LIMIT 80 OFFSET #{index * 80}
                                   SQL
      sql = []
      serviceData.each do |service|
        serviceAccountId = service[:ServiceAccountId].tr "-", ""
        dataSetName = "#{service[:ServiceId]}_#{serviceAccountId}"
        sql << "(SELECT 
      OnFacebookLeads,
      WebsiteRegistrationsCompleted,
      WebsiteContacts,
      WebsitePurchases,
      WebsiteApplicationsSubmitted,
      WebsiteLeads,
      AddToCart,
      TrialsStarted,
      '#{service[:PromotionName]}' AS PromotionName,
      '#{service[:PromotionId]}' AS PromotionId 
      FROM `#{dataSetName}.campaign` 
      WHERE 
      Date  BETWEEN '#{Date.today.prev_month(3).to_s}' AND '#{Date.today.to_s}'
      AND (OnFacebookLeads IS NOT NULL OR WebsiteRegistrationsCompleted IS NOT NULL OR WebsiteContacts IS NOT NULL OR WebsitePurchases IS NOT NULL OR WebsiteApplicationsSubmitted IS NOT NULL OR WebsiteLeads IS NOT NULL OR AddToCart IS NOT NULL  OR TrialsStarted IS NOT NULL ))"
      end
      sqlQuery = sql.join(" UNION ALL ")
      sqlQuery = "
    INSERT INTO
    `wacul-databeat.wacul_ad_test.cv_test_label` (PromotionName,
    PromotionId,
    OnFacebookLeads,
    WebsiteRegistrationsCompleted,
    WebsiteContacts,
    WebsitePurchases,
    WebsiteApplicationsSubmitted,
    WebsiteLeads,
    AddToCart,
    TrialsStarted,
    SortC)
    SELECT 
    PromotionName,
    PromotionId,
    SUM(OnFacebookLeads) AS OnFacebookLeads ,
    SUM(WebsiteRegistrationsCompleted) AS WebsiteRegistrationsCompleted ,
    SUM(WebsiteContacts) AS WebsiteContacts ,
    SUM(WebsitePurchases) AS WebsitePurchases ,
    SUM(WebsiteApplicationsSubmitted) AS WebsiteApplicationsSubmitted ,
    SUM(WebsiteLeads) AS WebsiteLeads ,
    SUM(AddToCart) AS AddToCart ,
    SUM(TrialsStarted) AS TrialsStarted ,
    ARRAY_TO_STRING([CASE
      WHEN SUM(OnFacebookLeads) IS NOT NULL THEN '1'
    ELSE
    NULL
  END
    ,CASE
      WHEN SUM(WebsiteRegistrationsCompleted) IS NOT NULL THEN '1'
    ELSE
    NULL
  END
    ,
    CASE
      WHEN SUM(WebsiteContacts) IS NOT NULL THEN '1'
    ELSE
    NULL
  END
    ,
    CASE
      WHEN SUM(WebsitePurchases) IS NOT NULL THEN '1'
    ELSE
    NULL
  END
    ,
    CASE
      WHEN SUM(WebsiteApplicationsSubmitted) IS NOT NULL THEN '1'
    ELSE
    NULL
  END
    ,
    CASE
      WHEN SUM(WebsiteLeads) IS NOT NULL THEN '1'
    ELSE
    NULL
  END
    ,
    CASE
      WHEN SUM(AddToCart) IS NOT NULL THEN '1'
    ELSE
    NULL
  END
    ,
    CASE
      WHEN SUM(TrialsStarted) IS NOT NULL THEN '1'
    ELSE
    NULL
  END
    ], '') AS SortC
    FROM (#{sqlQuery}) 
    GROUP BY PromotionName,PromotionId
    ORDER BY SortC DESC"
      bigquery.query sqlQuery
    end

    render json: { message: "Success" } and return
