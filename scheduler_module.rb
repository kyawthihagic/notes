include CommonFunction
require "date"

module SchedulerModule

  # The job status is set to fail,success.
  def updateJobStatus(job_id, status = nil)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # The job status is set to "fail,success".
    row = { id: job_id, finished_at: Time.new }

    row[:status] = status if status.present?

    # Get update query from the hash row.
    updateQuery =
      self.updateQueryParser [row], "#{APP_CONFIG["bigquery_dataset"]}.job"

    # Update the job status in the database.
    bigquery.query_job updateQuery[:query], params: updateQuery[:params]
  end

  #
  # Record the job status to the database.
  #
  # job_name - The name of the job.
  # &block - The block to be executed.
  def runJob(job_name, &block)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Generate a unique ID for the job.
    job_id = SecureRandom.uuid

    begin
      # The job status is set to "Running".
      row = { id: job_id, status: "running", created_at: Time.new }

      # Get insert query from the hash row.
      inserQuery =
        self.insertQueryParser [row],
                               "#{APP_CONFIG["bigquery_dataset"]}.job",
                               "id",
                               false

      # Insert the job into the database.
      bigquery.query inserQuery[:query], params: inserQuery[:params]

      # Call the block.
      block.call job_id

      self.updateJobStatus job_id, "success"
    rescue Exception => e
      # Log the error.
      Rails.logger.info e
      self.sendSlackMessageByDefault "Job: #{job_name} failed. \nError: #{e}"
      self.updateJobStatus job_id, "fail"
    end
  end

  def sendSlackMessageByDefault(message)
    begin
      client = Slack::Web::Client.new
      members = APP_CONFIG["default_slack_member"]
      channels = APP_CONFIG["default_slack_channel"]

      channels.each do |channel|
        begin
          accounts = members.map { |account| "<@#{account}>" }
          client.chat_postMessage(
            channel: channel,
            text: "#{accounts.join(" ")}\n#{message}",
            as_user: true,
            mrkdwn: true,
            icon_emoji: true,
          )
        rescue Exception => e
          Rails
            .logger.error "SendSlackMessageByDefault channel: #{channel}, members :#{members}, realMembers :#{realMembers} Error: #{e.message}"
        end
      end
    rescue Exception => e
      Rails
        .logger.error "SendSlackMessageByDefault channels: #{channels}, members :#{members} Error: #{e.message}"
    end
  end

  # Get all promotions from the bigquery database by promotion id.
  #
  # promotion_id - The promotion id.
  def getPromotionServices(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Get ServiceId,ServiceAccountId from the databeat_attribute.
    selectPromotionQuery = <<~SQL
      SELECT DISTINCT ServiceId,ServiceAccountId,ServiceNameJA  
      FROM `wacul-databeat.databeat.databeat_attribute` databeat_attribute 
      WHERE PromotionId = @promotion_id  
    SQL

    return(bigquery.query selectPromotionQuery,
                          params: {
                            promotion_id: promotion_id,
                          })
  end

  # Calculate the budget g is not specified.
  #
  # promotion_id - The promotion id.
  def calculateBudgetGIsNotSet(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Get budget_g id one from the databeat_attribute.
    selectQuery = <<~SQL
      SELECT budget_g.id 
      FROM `#{APP_CONFIG["bigquery_dataset"]}.budget_g` budget_g  
        WHERE budget_g.promotion_id = @promotion_id  
        AND budget_g.del_flg = 0 
        AND budget_g.budget_g_name != '#{I18n.t "budgetGSetting.nosettingBudgetGName"}'
        ORDER BY budget_g.created_at
        LIMIT 1
    SQL

    data = bigquery.query selectQuery, params: { promotion_id: promotion_id }

    return data.count == 0
  end

  def getCampaignQueryWithStatus(promotions, startDate, endDate)
    selectQuery = ""
    promotions.each do |promotion|
      selectRowNumberQuery = <<~SQL
        SELECT
          campaign.Date,
          campaign.CampaignStatus,
          campaign.CampaignId,
          campaign.CampaignName,
          campaign.ServiceId,
          '#{promotion[:ServiceNameJA]}' as ServiceNameJA,
          ROW_NUMBER() OVER(PARTITION BY campaign.CampaignId ORDER BY campaign.Date DESC ) AS row_number
        FROM
          `#{promotion[:ServiceId]}_#{promotion[:ServiceAccountId].tr "-", ""}`.campaign campaign
        WHERE
          Date BETWEEN '#{startDate}' AND '#{endDate}'
      SQL
      selectQuery <<
        "(SELECT * FROM (#{selectRowNumberQuery}) campaign WHERE row_number = 1) UNION ALL"
    end
    selectQuery = selectQuery.chomp "UNION ALL"
    return selectQuery
  end

  # Calculate the campaign is not specified.
  #
  # promotion_id - The promotion id.
  def calculateCampainIsNotSetBudgetG(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Get promotions from the databeat_attribute.
    promotions = self.getPromotionServices promotion_id

    # Generate a query to get the budget_g.
    budgetGDetailQuery = self.getBudgetGDetailQueryV2 promotion_id

    # Generate a query to get the conversion value.
    selectCampainConversionValueQuery =
      self.getCampaignQueryWithStatus promotions,
                                      Date.today.prev_month(3),
                                      Date.today

    # Generate a query to sum all the conversion value.
    selectQuery = <<~SQL
      SELECT
        campaign.CampaignId,
        campaign.ServiceId,
        campaign.CampaignName,
        campaign.CampaignStatus,
        budget_g.budget_g_id,
        campaign.ServiceNameJA
      FROM (#{selectCampainConversionValueQuery}) campaign
            LEFT JOIN (#{budgetGDetailQuery}) budget_g
            ON
              campaign.ServiceId = budget_g.service_id
              AND campaign.CampaignId = budget_g.campaign_id
            WHERE
              CampaignStatus IS NOT NULL
              AND CampaignStatus NOT IN ('PAUSED',
                'OFF')
              AND budget_g_id IS NULL
            GROUP BY
              campaign.CampaignId,
              campaign.ServiceId,
              campaign.CampaignName,
              budget_g.budget_g_id,
              campaign.CampaignStatus,
              campaign.ServiceNameJA
    SQL
    return bigquery.query selectQuery
  end

  # Calculate the yesterday budget was zero.
  #
  # promotion_id - The promotion id.
  def calculateYesterdayConversionValueisZero(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Get promotions from the databeat_attribute.
    promotions = self.getPromotionServices promotion_id

    # Generate a query to get the budget_g.
    budgetGDetailQuery = self.getBudgetGDetailQueryV2 promotion_id

    # Generate a query to get the conversion value.
    selectCampainConversionValueQuery =
      self.getConversionValueQueryByPromotions promotions,
                                               Date.today.prev_day(2),
                                               Date.today.prev_day(2)

    # Generate a query to sum all the conversion value.
    selectQuery = <<~SQL
      SELECT
      SUM(Cost) AS  Cost,
        COUNT(DISTINCT budget_g.budget_g_id) AS BudgetGCount,
      FROM  (#{selectCampainConversionValueQuery}) campaign
         RIGHT JOIN (#{budgetGDetailQuery}) budget_g
         ON
           campaign.ServiceId = budget_g.service_id
           AND campaign.CampaignId = budget_g.campaign_id
    SQL

    # Get the total sum conversion value.
    data = bigquery.query selectQuery

    #  Get query log
    if Rails.configuration.schedulerModule_is_query_log
      Rails
        .logger.info "calculateYesterdayConversionValueisZero: Promotion Id:#{promotion_id} , query : #{selectQuery} ,data: #{data}"
    end
    return({
            status: data.first[:BudgetGCount] > 0 && !(data.first[:Cost].to_f > 0),
            Cost: data.first[:Cost],
          })
  end

  # Calculate the current month cpa is higher than the previous month cpa.
  #
  # promotion_id - The promotion id.
  def calculateCurrentCPAisHigherThanLastMonthCPA(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Get promotions from the databeat_attribute.
    promotions = self.getPromotionServices promotion_id

    # Generate a query to get the conversion value by current month.
    currentMonthQuery =
      self.getConversionValueQueryByPromotions promotions,
                                               Date.today.prev_day,
                                               Date.today.prev_day

    # Generate a query to get the conversion value by previous month.
    lastMonthQuery =
      self.getConversionValueQueryByPromotions promotions,
                                               Date.today.prev_day.prev_month,
                                               Date.today.prev_day.prev_month

    # Generate a query to sum current month conversion value and previous month conversion value.
    selectQuery = <<~SQL
      SELECT (
          SELECT ( IF(SUM(Conversions) = 0, 0,  SUM(Cost)  / SUM(Conversions)) )
          FROM (#{currentMonthQuery})
          ) as CurrentMonth, 
          (
            SELECT (IF(SUM(Conversions) = 0, 0,  SUM(Cost)  / SUM(Conversions)) )
            FROM (#{lastMonthQuery})
          )  as LastMonth
    SQL

    data = bigquery.query selectQuery

    #  Get query log
    if Rails.configuration.schedulerModule_is_query_log
      Rails
        .logger.info "calculateCurrentCPAisHigherThanLastMonthCPA: Promotion Id:#{promotion_id} , query : #{selectQuery} ,data: #{data}"
    end

    return({
            CurrentMonthCPA: data.first[:CurrentMonth].to_f,
            LastMonthCPA: data.first[:LastMonth].to_f,
          })
  end

  # Calculate the current month cpa is higher than the target cpa.
  #
  # promotion_id - The promotion id.
  def calculateCurrentCPAisHigherThanTargetCPA(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Get promotions from the databeat_attribute.
    promotions = self.getPromotionServices promotion_id

    # Generate a query to get the conversion value by current month.
    currentMonthQuery =
      self.getConversionValueQueryByPromotions promotions,
                                               Date.today.beginning_of_month,
                                               Date.today.prev_day

    # Generate a query to get the target cpa.
    targetCPAQuery = <<~SQL
      SELECT target_cpa 
      FROM  `#{APP_CONFIG["bigquery_dataset"]}.target_cv_cpa` target_cv_cpa 
        WHERE promotion_id = '#{promotion_id}' 
        AND target_cv_cpa.del_flg = 0
    SQL

    # Generate a query to sum current month conversion value and target cpa.
    selectQuery = <<~SQL
      SELECT (
        SELECT (IF(SUM(Conversions) = 0, 0,  SUM(Cost)  / SUM(Conversions)))
        FROM (#{currentMonthQuery})
        ) as CurrentMonth, (
          SELECT SUM(target_cpa)
          FROM (#{targetCPAQuery})
        ) as target_cpa
    SQL

    data = bigquery.query selectQuery

    #  Get query log
    if Rails.configuration.schedulerModule_is_query_log
      Rails
        .logger.info "calculateCurrentCPAisHigherThanTargetCPA: Promotion Id:#{promotion_id} , query : #{selectQuery} ,data: #{data}"
    end

    return({
            status: data.first[:CurrentMonth].to_f > data.first[:target_cpa].to_f,
            CurrentMonthCPA: data.first[:CurrentMonth].to_f,
            target_cpa: data.first[:target_cpa].to_f,
          })
  end

  # Count the CV0 consecutive zero CV0 days.
  #
  # promotion_id - The promotion id.
  def calculateCV0ConsecutiveXdays(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Get promotions from the databeat_attribute.
    promotions = self.getPromotionServices promotion_id

    # Generate a query to get the conversion value by current month.
    currentMonthQuery =
      self.getConversionValueQueryByPromotions promotions,
                                               Date.today.beginning_of_month,
                                               Date.today.prev_day

    # Generate a query to get the conversion value by current month.
    selectQuery = <<~SQL
      SELECT
        SUM(Conversions) AS Conversions,
        Date
      FROM (#{currentMonthQuery})
        GROUP BY
          Date
        ORDER BY
          Date DESC
    SQL

    conversionData = bigquery.query selectQuery

    # Count the consecutive zero CV0 days.
    conversionValueZeroCount = 0
    (Date.today.beginning_of_month..Date.today.prev_day).reverse_each do |date|
      data = conversionData.find { |d| d[:Date] == date }
      if !(data.present? && data[:Conversions].to_f > 0)
        conversionValueZeroCount += 1
      else
        break
      end
    end

    #  Get query log
    if Rails.configuration.schedulerModule_is_query_log
      Rails
        .logger.info "calculateCV0ConsecutiveXdays: Promotion Id:#{promotion_id} , query : #{selectQuery} ,data: #{conversionData}"
    end

    return conversionValueZeroCount
  end

  # Calculate the budget
  # 1w：25％　15％～35％はOK　（前後10％）
  # 2W：50％　43％～57％はOK　（前後7％）
  # 3W：75％　72％～78％はOK　（前後3％）
  # 4W：100％ 97％～100％はOK　（前後3％）
  def calculateWeekBudget(campaign)
    weekRule = {
      1 => {
        from: 0.15,
        to: 0.35,
        value: 0.25,
      },
      2 => {
        from: 0.43,
        to: 0.57,
        value: 0.50,
      },
      3 => {
        from: 0.72,
        to: 0.78,
        value: 0.75,
      },
      4 => {
        from: 0.97,
        to: 1,
        value: 1,
      },
    }

    day = (Date.today.prev_day.day + 6) / 7

    week = day < 5 ? day : 4

    campaignWeekRule = weekRule[week]
    if campaign[:amount]
      expectedValue = campaign[:amount]
      return(campaign.merge(
              {
                expectedValue: expectedValue,
                expectedFromValue: expectedValue * campaignWeekRule[:from],
                expectedToValue: expectedValue * campaignWeekRule[:to],
                week: week,
              },
            ))
    end
    return campaign.merge({ week: week })
  end

  # Generate budget data for the budget_g_detail
  def getBudgetGDetailQuery(promotion_id)
    # Generate a query to get the budget_g_detail
    return <<~SQL
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          SELECT
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            (
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            SELECT
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              budget_g_detail_tar_cv
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            FROM
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              `#{APP_CONFIG["bigquery_dataset"]}.target_cv_cpa_detail`
          WHERE
            budget_g_id=budget_g.id
            AND del_flg = 0) AS total_amount,
          budget_g.id,
          budget_g_detail.service_id,
          budget_g_detail.campaign_id,
          budget_g.promotion_id,
          ( (
            SELECT
              budget_g_detail_tar_cv
            FROM
              `#{APP_CONFIG["bigquery_dataset"]}.target_cv_cpa_detail`
            WHERE
              budget_g_id=budget_g.id
              AND del_flg = 0) / (
            SELECT
              COUNT(budget_g_id)
            FROM
              `#{APP_CONFIG["bigquery_dataset"]}.budget_g_detail`
            WHERE
              budget_g_id = budget_g.id
              AND budget_g_detail.del_flg = 0)) AS amount,
        FROM
          `#{APP_CONFIG["bigquery_dataset"]}.budget_g_detail` budget_g_detail
        INNER JOIN
          `#{APP_CONFIG["bigquery_dataset"]}.budget_g` budget_g
        ON
          budget_g.id = budget_g_detail.budget_g_id
          AND budget_g.is_monthly_budget = 0
          AND budget_g.del_flg = 0
          AND budget_g.promotion_id = '#{promotion_id}'
        WHERE
          budget_g_detail.del_flg = 0
           SQL
  end

  # Generate budget data for the budget_g_detail
  def getBudgetGDetailQueryV2(promotion_id)
    # Generate a query to get the budget_g_detail
    return <<~SQL
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          SELECT
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          budget_g.id AS budget_g_id,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          budget_g_detail.service_id,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          budget_g_detail.campaign_id,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          budget_g.budget_g_name,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          amount,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          FROM
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            `#{APP_CONFIG["bigquery_dataset"]}.budget_g_detail` budget_g_detail
        INNER JOIN
          `#{APP_CONFIG["bigquery_dataset"]}.budget_g` budget_g
        ON
          budget_g.id = budget_g_detail.budget_g_id
          AND budget_g.is_monthly_budget = 0
          AND budget_g.del_flg = 0
          AND budget_g.promotion_id = '#{promotion_id}'
        WHERE
          budget_g_detail.del_flg = 0
           SQL
  end

  # Get Weekly budget data from the campaigm
  def getWeekCampainQuery(promotions)
    day = (Date.today.prev_day.day + 6) / 7
    return self.getWeekCampainQueryByDay promotions, day < 5 ? day : 4
  end

  # Get  1W~4W Campaign data from the campaigm
  def getWeekCampainQueryByDay(promotions, day)
    endDate = {
      1 => Date.today.prev_day.beginning_of_month + 6,
      2 => Date.today.prev_day.beginning_of_month + 13,
      3 => Date.today.prev_day.beginning_of_month + 20,
      4 => Date.today.prev_day.end_of_month,
    }
    comapinQuery =
      self.getConversionValueQueryByPromotions promotions,
                                               Date.today.prev_day
                                                 .beginning_of_month,
                                               endDate[day]

    #  Sum the conversion value by campaign_id
    return <<~SQL
                                                                                                                                                                                                                                                                                                                                                                                        SELECT
                                                                                                                                                                                                                                                                                                                                                                                          CampaignId,
                                                                                                                                                                                                                                                                                                                                                                                          ServiceId,
                                                                                                                                                                                                                                                                                                                                                                                          CampaignName,
                                                                                                                                                                                                                                                                                                                                                                                          SUM(Cost) AS Cost,
                                                                                                                                                                                                                                                                                                                                                                                        FROM (#{comapinQuery}) campaign
          GROUP BY
            CampaignId,
            CampaignName,
            ServiceId
           SQL
  end

  def calculateWeek(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Get promotions from the databeat_attribute.
    promotions = self.getPromotionServices promotion_id
    campaignQuery = self.getWeekCampainQuery promotions
    budgetGDetailQuery = self.getBudgetGDetailQuery promotion_id

    # Generate a query to sum current month conversion value and target cpa.
    selectQuery = <<~SQL
      SELECT
        campaign.*,
        budget_g.*
      FROM (#{campaignQuery}) campaign
        INNER JOIN (#{budgetGDetailQuery}) budget_g
        ON
          campaign.ServiceId = budget_g.service_id
          AND campaign.CampaignId = budget_g.campaign_id
        ORDER BY campaign.week ASC
    SQL
    return selectQuery
    data = bigquery.query selectQuery
    return data.map { |row| self.calculateWeekBudget row }
  end

  def calculateAlertCost(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Get promotions from the databeat_attribute.
    promotions = self.getPromotionServices promotion_id
    campaignQuery = self.getConversionValueQueryByPromotions promotions, getWokingDay(Date.today)
    budgetGDetailQuery = self.getBudgetGDetailQueryV2 promotion_id

    # Generate a query to sum current month conversion value and target cpa.
    selectQuery = <<~SQL
      SELECT
        SUM(Cost) AS cost,
        budget_g.budget_g_id,
        budget_g.budget_g_name,
        budget_g.amount,
      FROM  (#{campaignQuery}) campaign
      RIGHT JOIN (#{budgetGDetailQuery}) budget_g
      ON
        campaign.ServiceId = budget_g.service_id
        AND campaign.CampaignId = budget_g.campaign_id
      GROUP BY
        budget_g.budget_g_id,
        budget_g.budget_g_name,
        budget_g.amount
    SQL

    data = bigquery.query selectQuery
    data = data.map { |row| self.calculateBudget row }
    #  Get query log
    if Rails.configuration.schedulerModule_is_query_log
      Rails
        .logger.info "calculateAlertCost: Promotion Id:#{promotion_id} , query : #{selectQuery} ,data: #{data}"
    end
    return data
  end

  # Get PromotionId,PromotionName from the databeat_attribute.
  def getPromotionDataSchdeulder
    # BigQueryと接続
    bigquery = self.connectBigQuery

    # 契約ステータス「解約済、非表示」でないプロモーション名をフィルターするクエリ
    contractStatusQuery =
      "SELECT contract_status_table.promotion_id FROM #{APP_CONFIG["bigquery_dataset"]}.contract_status contract_status_table WHERE contract_status_table.contract_status IN (1,2) AND del_flg = 0"

    # Generate a query to get the promotion data.
    selectQuery = <<~SQL
      SELECT DISTINCT PromotionId,PromotionName  
      FROM `wacul-databeat.databeat.databeat_attribute` databeat_attribute 
      WHERE databeat_attribute.PromotionId 
      NOT IN (#{contractStatusQuery}) 
        ORDER BY databeat_attribute.PromotionName
    SQL

    return bigquery.query selectQuery
  end

  # Get slack channel id from the slack_channels table.
  # promotion_id - The promotion id.
  def getSlackChannel(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Generate a query to get the slack channel id.
    selectQuery = <<~SQL
      SELECT channel_id 
      FROM `#{APP_CONFIG["bigquery_dataset"]}.slack_channels` 
        WHERE del_flg = 0 
        AND promotion_id = @promotionId
    SQL

    return bigquery.query selectQuery, params: { promotionId: promotion_id }
  end

  # Get slack member id from the person_in_charge table.
  # promotion_id - The promotion id.
  def getTantouMaster(promotion_id)
    #BigQueryと接続
    bigquery = self.connectBigQuery

    # Generate a slack front member id from the person_in_charge table.
    selectFrontQuery = <<~SQL
      SELECT slack_account 
      FROM `#{APP_CONFIG["bigquery_dataset"]}.person_in_charge` 
        WHERE del_flg = 0 
        AND id = front_id
    SQL

    # Generate a slack back member id from the person_in_charge table.
    selectBackQuery = <<~SQL
      SELECT slack_account 
      FROM `#{APP_CONFIG["bigquery_dataset"]}.person_in_charge` 
        WHERE del_flg = 0 
        AND id = back_id
    SQL

    # Generate a query to get the slack member id.
    selectQuery = <<~SQL
      SELECT (#{selectFrontQuery}) as front_slack_account,(#{selectBackQuery}) as back_slack_account   
      FROM `#{APP_CONFIG["bigquery_dataset"]}.promotion_person_in_charge` 
      WHERE del_flg = 0 
      AND promotion_id = @promotionId
    SQL

    return bigquery.query selectQuery, params: { promotionId: promotion_id }
  end

  # Send a message to the slack channel.
  #
  # promotion_id - The promotion id.
  # message - The message to send.
  def sendMessage(promotion_id, message)
    # Get slack member id from the person_in_charge table.
    users = self.getTantouMaster promotion_id

    # Get slack channel id from the slack_channels table.
    channels = self.getSlackChannel promotion_id

    client = Slack::Web::Client.new

    # Convert back member id and front member id to slack member id.
    members =
      APP_CONFIG["default_slack_member"] +
        users.map { |user| user[:front_slack_account] } +
        users.map { |user| user[:back_slack_account] }

    # Unique slack member id.
    members =
      members.uniq.reject do |item|
        item.nil? || item == "" || (item && item.length < 5)
      end

    # Map channel obj to channel id.
    channels =
      channels.map { |channel| channel[:channel_id] } +
        APP_CONFIG["default_slack_channel"]

    # Unique slack channel id.
    channels = channels.uniq { |channel| channel }

    slackErrors = []

    # Send a message to the slack channel.
    channels.each do |channel|
      accounts = members.map { |account| "<@#{account}>" }
      begin
        client.chat_postMessage(
          channel: channel,
          text: "#{accounts.join(" ")}\n#{message}",
          as_user: true,
          mrkdwn: true,
          icon_emoji: true,
        )
      rescue Exception => e
        slackErrors << "channel(#{channel}) is error(#{e.message})"
      end
    end

    raise StandardError.new(slackErrors.join(", ")) if slackErrors.length > 0

    return({
            message: message,
            users: users,
            slackUserIds: members,
            channels: channels,
          })
  end

  def formatNumber(number)
    return number.to_f.round(2).to_s.gsub(/\d(?=(\d{3})+\.)/, '\&,')
  end

  # Generate Message for each campaign.
  def caculateWeekCVmessage(weekCVData)
    message = ""
    weekCVData
      .group_by { |data| "#{data[:CampaignId]}-#{data[:ServiceId]}" }
      .transform_values
      .each do |data|
      campainMessage = ""
      data.each do |weekCV|
        if weekCV[:ConversionValue].to_f < weekCV[:expectedFromValue].to_f
          campainMessage <<
            "#{I18n.t "schedulerModule.progressDelayed", week: weekCV[:week], campaignName: weekCV[:CampaignName]} (#{self.formatNumber(weekCV[:expectedValue])}) (#{self.formatNumber(weekCV[:expectedFromValue])} < #{self.formatNumber(weekCV[:ConversionValue])} < #{self.formatNumber(weekCV[:expectedToValue])})\n"
        elsif weekCV[:ConversionValue].to_f > weekCV[:expectedToValue].to_f
          campainMessage <<
            "#{I18n.t "schedulerModule.progressOver", week: weekCV[:week], campaignName: weekCV[:CampaignName]} (#{self.formatNumber(weekCV[:expectedValue])}) (#{self.formatNumber(weekCV[:expectedFromValue])} < #{self.formatNumber(weekCV[:ConversionValue])} < #{self.formatNumber(weekCV[:expectedToValue])})\n"
        end
      end
      if campainMessage.length > 0
        campainMessage << "\n"
        message << campainMessage
      end
    end
    return message
  end

  # Generate Message for each budget.
  def calculateAlertCostMessage(data)
    campainMessage = ""
    data.each do |item|
      if item[:over_below_budget_alert].present?
        campainMessage << item[:over_below_budget_alert] << "\n"
      end
      if item[:below_cv_alert].present?
        campainMessage << item[:below_cv_alert] << "\n"
      end
      if item[:over_target_cpa_alert].present?
        campainMessage << item[:over_target_cpa_alert] << "\n"
      end
    end
    return campainMessage
  end

  # Generate slack message.
  #
  # promotion_id - The promotion id.
  def calculateMessage(promotion)
    message = ""

    budgetGIsNotSet = self.calculateBudgetGIsNotSet promotion[:PromotionId]
    yesterdayConversionValueisZero =
      self.calculateYesterdayConversionValueisZero promotion[:PromotionId]

    cvZeroConsecutiveXdays =
      self.calculateCV0ConsecutiveXdays promotion[:PromotionId]

    if budgetGIsNotSet
      message <<
        "#{I18n.t "message.M036", promotionName: promotion[:PromotionName]}\n"
    else
      campaignBudgetGIsNotSetData =
        self.calculateCampainIsNotSetBudgetG promotion[:PromotionId]
      campaignBudgetGIsNotSetData.each do |campain|
        message <<
          "#{I18n.t "message.M037", mediumName: campain[:ServiceNameJA], campaignName: campain[:CampaignName]}\n"
      end
    end

    if yesterdayConversionValueisZero[:status]
      message << "#{I18n.t "message.M038"}\n"
    end

    # Timing
    if isTimingDay
      data = self.calculateAlertCost promotion[:PromotionId]
      alert_cost_message = self.calculateAlertCostMessage data
      message << alert_cost_message if alert_cost_message.length > 0
    end
    # currentCPAisHigherThanLastMonthCPA =
    # self.calculateCurrentCPAisHigherThanLastMonthCPA promotion[:PromotionId]

    # if currentCPAisHigherThanLastMonthCPA[:CurrentMonthCPA] > currentCPAisHigherThanLastMonthCPA[:LastMonthCPA]
    #   message << "#{I18n.t "message.M042"}\n"
    # elsif currentCPAisHigherThanLastMonthCPA[:CurrentMonthCPA] < currentCPAisHigherThanLastMonthCPA[:LastMonthCPA]
    #   message << "#{I18n.t "message.M041"}\n"
    # elsif currentCPAisHigherThanLastMonthCPA[:CurrentMonthCPA] == currentCPAisHigherThanLastMonthCPA[:LastMonthCPA]
    #   message << "#{I18n.t "message.M048"}\n"
    # end

    # Timing
    # if isTimingDay && false
    #   currentCPAisHigherThanTargetCPA =
    #     self.calculateCurrentCPAisHigherThanTargetCPA promotion[:PromotionId]
    #   if currentCPAisHigherThanTargetCPA[:target_cpa] == 0
    #     message << "#{I18n.t "message.M049", promotionName: promotion[:PromotionName]}\n"
    #   elsif currentCPAisHigherThanTargetCPA[:CurrentMonthCPA] > currentCPAisHigherThanTargetCPA[:target_cpa]
    #     message << "#{I18n.t "message.M044"}\n"
    #   elsif currentCPAisHigherThanTargetCPA[:CurrentMonthCPA] < currentCPAisHigherThanTargetCPA[:target_cpa]
    #     message << "#{I18n.t "message.M043"}\n"
    #   elsif currentCPAisHigherThanTargetCPA[:CurrentMonthCPA] == currentCPAisHigherThanTargetCPA[:target_cpa]
    #     message << "#{I18n.t "message.M050"}\n"
    #   end
    # end

    if cvZeroConsecutiveXdays > 0
      message << "#{I18n.t "message.M045", days: cvZeroConsecutiveXdays}\n"
    end

    Rails
      .logger.info "send message to #{promotion[:PromotionId]} #{promotion[:PromotionName]} #{message} "
    Rails
      .logger.info "PromotionName #{promotion[:PromotionName]}, budgetGIsNotSet #{budgetGIsNotSet} yesterdayConversionValueisZero #{yesterdayConversionValueisZero}  cvZeroConsecutiveXdays #{cvZeroConsecutiveXdays} "
    if message.length > 0
      return "#{promotion[:PromotionName]}\n" + message
    end
    return false
  end

  def main
    Rails.configuration.schedulerModule_is_query_log = false
    self.runJob "Notify Promotion Daily Midnight" do |parent_job_id|
      promotions = self.getPromotionDataSchdeulder
      Rails.configuration.serviceNameTableSetting =
        self.getServiceNameTableSetting
      promotions.each do |promotion|
        SchedulerJob.perform_later(promotion, parent_job_id)
      end
    end
  end

  # Testing Only slack noti by Promotion Id
  def testByPromotionId()
    Rails.configuration.schedulerModule_is_query_log = true
    self.runJob "Notify Promotion Daily Midnight" do |parent_job_id|
      promotions = [
        {
          PromotionId: "5b301b91-bb66-4b36-b894-41b8a00e8684",
          PromotionName: "AIアナリスト",
        },
      ]
      Rails.configuration.serviceNameTableSetting =
        self.getServiceNameTableSetting
      promotions.each do |promotion|
        SchedulerJob.perform_later(promotion, parent_job_id)
      end
    end
  end

  def calculateBudget(budget_g, date = Date.today)
    budget = budget_g[:amount]
    if budget <= 0
      return {}
    end
    cost = budget_g[:cost]
    weekDays = self.getWokingDay()
    prediction_date = weekDays.select { |item| item <= date }.last
    index = self.getTimingWorkDay(weekDays).find_index(prediction_date)
    logger.info "getValueindex budget##{budget} cost##{cost} #{index} #{weekDays.count} #{prediction_date} #{weekDays}"
    formula = self.getTimingAmountFormula[index]
    expected_allowable_cost = self.getTimingBelowCV[index]
    expected_over_taget_cpa_allowable_cost = self.getTimingOverTargetCPA[index]
    expected_over_below_budget_allowable_cost = self.getTimingBudgetOverBelow[index]
    expected_cost = formula.(cost.to_f, weekDays.count.to_f)
    budget_progress = expected_cost.to_f / budget.to_f * 100.0
    over_below_budget_alert = nil
    if budget_progress < expected_over_below_budget_allowable_cost[0]
      over_below_budget_alert = I18n.t("message.M039", budgetGName: budget_g[:budget_g_name])
    elsif budget_progress > expected_over_below_budget_allowable_cost[1]
      over_below_budget_alert = I18n.t("message.M040", budgetGName: budget_g[:budget_g_name])
    end

    return ({
             budget: budget.to_f.round,
             cost: cost.to_f.round,
             expected_cost: expected_cost.round,
             budget_progress: budget_progress.round,
             expected_allowable_cost: expected_allowable_cost,
             below_cv_alert: budget_progress >= expected_allowable_cost ? nil : I18n.t("message.M051", budgetGName: budget_g[:budget_g_name]),
             over_target_cpa_alert: expected_over_taget_cpa_allowable_cost == 0 || expected_over_taget_cpa_allowable_cost >= budget_progress ? nil : I18n.t("message.M052", budgetGName: budget_g[:budget_g_name]),
             over_below_budget_alert: over_below_budget_alert,
           })
  end

  def getTimingBudgetOverBelow()
    return [[60.0, 140.0], [71.2, 128.8], [80.8, 119.2], [90.4, 109.6], [95.2, 104.8], [96.8, 103.2], [98.4, 101.6]]
  end

  def getTimingOverTargetCPA()
    return [130.0, 120.0, 120.0, 115.0, 110.0, 0.0, 0.0]
  end

  def getTimingBelowCV()
    return [60.0, 68.0, 76.0, 84.0, 95.2, 96.8, 98.4]
  end

  def getTimingAmountFormula()
    return [
             lambda { |amount, month| amount * 5 },
             lambda { |amount, month| amount * 2.5 },
             lambda { |amount, month| amount * 5 / 3 },
             lambda { |amount, month| amount * 1.25 },
             lambda { |amount, month| (amount * month) / (month - 3) },
             lambda { |amount, month| amount * month / (month - 2) },
             lambda { |amount, month| amount * month / (month - 1) },
           ]
  end

  def getTimings(month)
    return [
             (month / 5.0).round,
             (2 * month / 5.0).round,
             (3 * month / 5.0).round,
             (4 * month / 5.0).round,
             (month - 3),
             (month - 2),
             (month - 1),
           ]
  end

  def getWokingDay(endDate = Date.today.end_of_month, startDate = Date.today.beginning_of_month)
    weekDays =
      (startDate..endDate).reject do |day|
        day.saturday? || day.sunday? || HolidayJapan.check(day)
      end
    return weekDays
  end

  def getTimingWorkDay(weekDays = self.getWokingDay)
    return getTimings(weekDays.count).map { |day| weekDays[day - 1] }
  end

  def getTimingDay()
    date = Date.parse("2022-08-4")
    timings = getTimings(date.end_of_month.day)
    timingDays = timings.map do |day|
      Date.strptime("#{date.strftime("%Y-%m")}-#{day}", "%Y-%m-%d")
    end
    return timingDays
  end

  def isTimingDay
    date = Date.today
    return getTimingWorkDay.include?(date)
  end

  # プロモーションと予算により全体のプロモーションデータを登録する
  def promotionData
    puts "全体のプロモーションデータ取得Bash開始"
    #今日の日付
    #開始日 （前月の一日）
    startDate = Date.today.last_month.at_beginning_of_month.to_s
    sDate = Date.today.last_month.at_beginning_of_month

    #  本日
    todayDate = Date.today.to_s

    # 今月の開始日
    startDateOfThisMonth = Date.today.at_beginning_of_month

    #終了日（当日）
    endDate = Date.today.to_s
    eDate = Date.today

    #BigQueryと接続
    bigquery = self.connectBigQuery

    # ジョブデータ登録
    # ジョブId作成
    job_id = SecureRandom.uuid

    # The job status is set to "Running".
    row = { id: job_id, status: "running", created_at: Time.new, jobType: 2 }

    # Get insert query from the hash row.
    inserQuery =
      self.insertQueryParser [row],
                             "#{APP_CONFIG["bigquery_dataset"]}.job",
                             "id",
                             false

    # Insert the job into the database.
    bigquery.query inserQuery[:query], params: inserQuery[:params]

    #予算Gデーだ取得
    budgetgData = bigquery.query <<~SQL
                                   SELECT 
                                     budgetg.id budget_g_id,
                                     budgetg.budget_g_name,
                                     budgetg.amount amount,
                                     budgetg.is_monthly_budget is_monthly_budget,
                                     budgetg.del_flg,
                                     budgetg.promotion_id PromotionId,
                                     detail.id budget_g_detail_id,
                                     detail.campaign_id campaign_id,
                                     detail.service_id service_id
                                   FROM 
                                     #{APP_CONFIG["bigquery_dataset"]}.budget_g budgetg 
                                    JOIN 
                                      #{APP_CONFIG["bigquery_dataset"]}.budget_g_detail detail 
                                    ON detail.budget_g_id=budgetg.id 
                                    WHERE budgetg.del_flg = 0
                                    AND is_monthly_budget = 0
                                 SQL

    # 契約ステータス「契約中」のデータを取得
    contractStatusQuery = <<~SQL
      SELECT c.promotion_id 
      FROM #{APP_CONFIG["bigquery_dataset"]}.contract_status c 
                          WHERE c.contract_status IN (1,2) 
                          AND del_flg = 0
    SQL

    # 媒体サービスデータ取得取得
    serviceNameTableSetting = bigquery.query <<~SQL
                                                                                                                                                                                                                                                                                                                                                                                                                          SELECT
                                                                                                                                                                                                                                                                                                                                                                                                                          *
                                                                                                                                                                                                                                                                                                                                                                                                                          FROM
                                                                                                                                                                                                                                                                                                                                                                                                                          `#{APP_CONFIG["bigquery_dataset"]}.service_name_table_setting` service_name_table_setting
                                            WHERE
                                            del_flg = 0
                                             SQL

    #プロモーションデータ取得
    promotionData = bigquery.query self.loadPromotionData contractStatusQuery
    groupPromotionData = promotionData.group_by { |x| x[:PromotionId] }.values

    # # データセット
    # dataset = bigquery.dataset"#{APP_CONFIG['bigquery_dataset']}"
    # table = dataset.table"all_promotiondata"

    # 更新するためデータを削除
    deleteQuery = "DELETE FROM #{APP_CONFIG["bigquery_dataset"]}.all_promotiondata WHERE del_flg = 0"
    bigquery.query deleteQuery

    promotionUsageAmount = []
    promotionsUsageAmount = []

    if budgetgData.length > 0
      #利用金額取得
      groupPromotionData.each do |promotion|
        sql = self.getAmount promotion, budgetgData, startDate, endDate, serviceNameTableSetting
        if sql != ""
          usageAmount = bigquery.query sql.chomp! " UNION ALL "
          amount = 0
          conversionValue = 0
          conversionActions0 = 0
          conversionActions1 = 0
          conversionActions2 = 0
          conversionActions3 = 0
          conversionActions4 = 0
          conversionActions5 = 0
          conversionActions6 = 0
          conversionActions7 = 0
          conversionActions8 = 0
          conversionActions9 = 0
          conversionActions10 = 0
          conversionActions11 = 0
          conversionActions12 = 0
          conversionActions13 = 0
          conversionActions14 = 0
          conversionActions15 = 0
          conversionActions16 = 0
          conversionActions17 = 0
          conversionActions18 = 0
          conversionActions19 = 0
          conversionActions20 = 0
          conversionActions21 = 0
          conversionActions22 = 0
          conversionActions23 = 0
          conversionActions24 = 0
          conversionActions25 = 0
          conversionActions26 = 0
          conversionActions27 = 0
          conversionActions28 = 0
          conversionActions29 = 0
          conversionActions30 = 0
          conversionActions31 = 0
          conversionActions32 = 0
          conversionActions33 = 0
          conversionActions34 = 0
          conversionActions35 = 0
          conversionActions36 = 0
          conversionActions37 = 0
          conversionActions38 = 0
          conversionActions39 = 0
          conversionActions40 = 0
          conversionActions41 = 0
          conversionActions42 = 0
          conversionActions43 = 0
          conversionActions44 = 0
          conversionActions45 = 0
          conversionActions46 = 0
          conversionActions47 = 0
          conversionActions48 = 0
          conversionActions49 = 0
          conversionActions50 = 0
          conversionActions51 = 0
          conversionActions52 = 0
          conversionActions53 = 0
          conversionActions54 = 0
          conversionActions55 = 0
          conversionActions56 = 0
          conversionActions57 = 0
          conversionActions58 = 0
          conversionActions59 = 0
          conversionActions60 = 0
          conversionActions61 = 0
          conversionActions62 = 0
          conversionActions63 = 0
          conversionActions64 = 0
          conversionActions65 = 0
          conversionActions66 = 0
          conversionActions67 = 0
          conversionActions68 = 0
          conversionActions69 = 0
          conversionActions70 = 0
          conversionActions71 = 0
          conversionActions72 = 0
          conversionActions73 = 0
          conversionActions74 = 0
          conversionActions75 = 0
          conversionActions76 = 0
          conversionActions77 = 0
          conversionActions78 = 0
          conversionActions79 = 0
          conversionActions80 = 0
          conversionActions81 = 0
          conversionActions82 = 0
          conversionActions83 = 0
          conversionActions84 = 0
          conversionActions85 = 0
          conversionActions86 = 0
          conversionActions87 = 0
          conversionActions88 = 0
          conversionActions89 = 0
          conversionActions90 = 0
          conversionActions91 = 0
          conversionActions92 = 0
          conversionActions93 = 0
          conversionActions94 = 0
          conversionActions95 = 0
          conversionActions96 = 0
          conversionActions97 = 0
          conversionActions98 = 0
          conversionActions99 = 0
          promotionId = ""
          isMonthlyBudget = 0
          dataByDateAmount = []

          while sDate != eDate
            dataByDate = usageAmount.select { |amount| amount[:Date] == sDate }
            if dataByDate.length > 0
              dataByDate.each do |data|
                conversionValue = conversionValue + data[:ConversionValue]
                amount = amount + data[:Amount]
                promotionId = data[:promotionId]
              end
            end
            dataByDateAmount << {
              "ConversionValue" => conversionValue,
              "ConversionTypeName" => "",
              "ConversionActions0" => conversionActions0,
              "ConversionActions1" => conversionActions1,
              "ConversionActions2" => conversionActions2,
              "ConversionActions3" => conversionActions3,
              "ConversionActions4" => conversionActions4,
              "ConversionActions5" => conversionActions5,
              "ConversionActions6" => conversionActions6,
              "ConversionActions7" => conversionActions7,
              "ConversionActions8" => conversionActions8,
              "ConversionActions9" => conversionActions9,
              "ConversionActions10" => conversionActions10,
              "ConversionActions11" => conversionActions11,
              "ConversionActions12" => conversionActions12,
              "ConversionActions13" => conversionActions13,
              "ConversionActions14" => conversionActions14,
              "ConversionActions15" => conversionActions15,
              "ConversionActions16" => conversionActions16,
              "ConversionActions17" => conversionActions17,
              "ConversionActions18" => conversionActions18,
              "ConversionActions19" => conversionActions19,
              "ConversionActions20" => conversionActions20,
              "ConversionActions21" => conversionActions21,
              "ConversionActions22" => conversionActions22,
              "ConversionActions23" => conversionActions23,
              "ConversionActions24" => conversionActions24,
              "ConversionActions25" => conversionActions25,
              "ConversionActions26" => conversionActions26,
              "ConversionActions27" => conversionActions27,
              "ConversionActions28" => conversionActions28,
              "ConversionActions29" => conversionActions29,
              "ConversionActions30" => conversionActions30,
              "ConversionActions31" => conversionActions31,
              "ConversionActions32" => conversionActions32,
              "ConversionActions33" => conversionActions33,
              "ConversionActions34" => conversionActions34,
              "ConversionActions35" => conversionActions35,
              "ConversionActions36" => conversionActions36,
              "ConversionActions37" => conversionActions37,
              "ConversionActions38" => conversionActions38,
              "ConversionActions39" => conversionActions39,
              "ConversionActions40" => conversionActions40,
              "ConversionActions41" => conversionActions41,
              "ConversionActions42" => conversionActions42,
              "ConversionActions43" => conversionActions43,
              "ConversionActions44" => conversionActions44,
              "ConversionActions45" => conversionActions45,
              "ConversionActions46" => conversionActions46,
              "ConversionActions47" => conversionActions47,
              "ConversionActions48" => conversionActions48,
              "ConversionActions49" => conversionActions49,
              "ConversionActions50" => conversionActions50,
              "ConversionActions51" => conversionActions51,
              "ConversionActions52" => conversionActions52,
              "ConversionActions53" => conversionActions53,
              "ConversionActions54" => conversionActions54,
              "ConversionActions55" => conversionActions55,
              "ConversionActions56" => conversionActions56,
              "ConversionActions57" => conversionActions57,
              "ConversionActions58" => conversionActions58,
              "ConversionActions59" => conversionActions59,
              "ConversionActions60" => conversionActions60,
              "ConversionActions61" => conversionActions61,
              "ConversionActions62" => conversionActions62,
              "ConversionActions63" => conversionActions63,
              "ConversionActions64" => conversionActions64,
              "ConversionActions65" => conversionActions65,
              "ConversionActions66" => conversionActions66,
              "ConversionActions67" => conversionActions67,
              "ConversionActions68" => conversionActions68,
              "ConversionActions69" => conversionActions69,
              "ConversionActions70" => conversionActions70,
              "ConversionActions71" => conversionActions71,
              "ConversionActions72" => conversionActions72,
              "ConversionActions73" => conversionActions73,
              "ConversionActions74" => conversionActions74,
              "ConversionActions75" => conversionActions75,
              "ConversionActions76" => conversionActions76,
              "ConversionActions77" => conversionActions77,
              "ConversionActions78" => conversionActions78,
              "ConversionActions79" => conversionActions79,
              "ConversionActions80" => conversionActions80,
              "ConversionActions81" => conversionActions81,
              "ConversionActions82" => conversionActions82,
              "ConversionActions83" => conversionActions83,
              "ConversionActions84" => conversionActions84,
              "ConversionActions85" => conversionActions85,
              "ConversionActions86" => conversionActions86,
              "ConversionActions87" => conversionActions87,
              "ConversionActions88" => conversionActions88,
              "ConversionActions89" => conversionActions89,
              "ConversionActions90" => conversionActions90,
              "ConversionActions91" => conversionActions91,
              "ConversionActions92" => conversionActions92,
              "ConversionActions93" => conversionActions93,
              "ConversionActions94" => conversionActions94,
              "ConversionActions95" => conversionActions95,
              "ConversionActions96" => conversionActions96,
              "ConversionActions97" => conversionActions97,
              "ConversionActions98" => conversionActions98,
              "ConversionActions99" => conversionActions99,
              "promotionId" => promotionId,
              "isMonthlyBudget" => 0,
              "Amount" => amount,
              "Date" => sDate,
              "CampaignId" => "",
              "CampaignName" => "",
              "del_flg" => 0,
            }
            sDate = sDate + 1
            amount = 0
            conversionValue = 0
          end

          sDate = Date.today.last_month.at_beginning_of_month
          eDate = Date.today

          promotionsUsageAmount << dataByDateAmount
          if dataByDateAmount.length > 0
            rows = self.getInsertData dataByDateAmount
            multipleQuery = []
            # CV-G設定データ登録
            cvgDataInsertQuery =
              self.insertPromotionDetailQueryParser(
                rows,
                "#{APP_CONFIG["bigquery_dataset"]}.all_promotiondata",
              )
            multipleQuery << cvgDataInsertQuery
            query = self.executeQueryJobTransaction bigquery, multipleQuery
            #table.insert rows
          end
        end
      end
    end
    self.updateJobStatus job_id, "success"
    puts "全体のプロモーションデータ取得Bash完了"
  end

  #プロモーションデータ取得
  def loadPromotionData(contractStatusQuery)
    # プロモーションIdによりアラートデータ取得
    return <<~SQL
             SELECT
                 DISTINCT databeat.PromotionId,
                 databeat.PromotionName,
                 databeat.ServiceAccountId,
                 databeat.ServiceId,
                 cpa.target_cpa,
                 cpa.target_cv,
                 cpa.amount,
               FROM
                 `wacul-databeat.databeat.databeat_attribute` databeat
               FULL OUTER JOIN
                 #{APP_CONFIG["bigquery_dataset"]}.target_cv_cpa cpa  
                ON
                  cpa.promotion_id = databeat.PromotionId
                WHERE
                  PromotionId NOT IN (#{contractStatusQuery}) order by PromotionName
           SQL
  end

  def getAmount(promotions, budgetgData, startDate, endDate, serviceNameTableSetting)
    selectQuery = ""
    promotions.each do |data|
      #不要文字を削除する
      #例：128-295-5819　⇒　1282955819
      serviceAccountId = data[:ServiceAccountId].tr "-", ""
      dataSetName = "#{data[:ServiceId]}_#{serviceAccountId}"
      budgetgData.each do |budgetg|
        if data[:PromotionId] === budgetg[:PromotionId] &&
           data[:ServiceId] === budgetg[:service_id]
          serviceName =
            serviceNameTableSetting.find do |x|
              x[:service_id] == data[:ServiceId]
            end
          table = "campaign"
          if serviceName.present? && serviceName[:table_name].present? &&
             %w[campaign_conversion campaign].include?(
               serviceName[:table_name],
             )
            table = serviceName[:table_name]
          end
          sql =
            self.getUsageAmount dataSetName,
                                table,
                                startDate,
                                endDate,
                                budgetg[:campaign_id],
                                budgetg[:PromotionId],
                                budgetg[:is_monthly_budget]
          selectQuery << "(#{sql}) UNION ALL "
        end
      end
    end
    return selectQuery
  end

  #プロモーション名により利用金額取得クエリ
  def getUsageAmount(
    dataSetName,
    tableName,
    startDate,
    endDate,
    campaignId,
    promotionId,
    isMonthlyBudget
  )
    selectQuery = ""
    if tableName == "campaign"
      query = <<~SQL
        SELECT
          #{self.getConversionValueColumnPromotionList(tableName)},             
          '#{promotionId}' as promotionId,
          '#{isMonthlyBudget}' as isMonthlyBudget,
          Cost As Amount,
          campaign.Date As Date,
          campaign.CampaignId As CampaignId ,
          campaign.CampaignName As CampaignName,              
        FROM
            `#{dataSetName}.#{tableName}` campaign
        WHERE
            Date BETWEEN '#{startDate}' AND '#{endDate}'
            AND campaign.CampaignId = '#{campaignId}'
            ORDER BY Date   
      SQL
      return query
    else
      query = <<~SQL
        SELECT
        0 AS ConversionValue,'' AS ConversionTypeName,#{(0..99).map { |i| "0 AS ConversionActions#{i}" }.join(",")},
        '#{promotionId}' as promotionId,
        '#{isMonthlyBudget}' as isMonthlyBudget,
        Cost As Amount,
        campaign.Date As Date,
        campaign.CampaignId As CampaignId,
        campaign.CampaignName As CampaignName,
        FROM
            `#{dataSetName}.campaign` campaign
        WHERE
            Date BETWEEN '#{startDate}' AND '#{endDate}'
            AND campaign.CampaignId = '#{campaignId}'
            ORDER BY Date 
      SQL
      selectQuery << " (#{query}) UNION ALL "

      query = <<~SQL
        SELECT
          #{self.getConversionValueColumnPromotionList(tableName)},
          '#{promotionId}' as promotionId,
          '#{isMonthlyBudget}' as isMonthlyBudget,            
          0 AS Amount,
          campaign.Date As Date,
          campaign.CampaignId As CampaignId,
          campaign.CampaignName As CampaignName
        FROM
            `#{dataSetName}.campaign_conversion` campaign
        WHERE
            Date BETWEEN '#{startDate}' AND '#{endDate}'
            AND campaign.CampaignId = '#{campaignId}'
            ORDER BY Date 
      SQL
      selectQuery << "(#{query})"
      return selectQuery
    end
  end

  def getConversionValueColumnPromotionList(tableName)
    if tableName == "campaign_conversion"
      return "Conversions as ConversionValue,ConversionTypeName,#{(0..99).map { |i| "0 AS ConversionActions#{i}" }.join(",")}"
    end

    return "0 AS ConversionValue,'' AS ConversionTypeName,#{(0..99).map { |i| "ConversionActions#{i}" }.join(",")}"
  end

  def getInsertData(usageAmount)
    return rows =
             usageAmount
               .map do |row|
               {
                 "ConversionValue" => row["ConversionValue"],
                 "ConversionTypeName" => row["ConversionTypeName"],
                 "ConversionActions0" => row["ConversionActions0"],
                 "ConversionActions1" => row["ConversionActions1"],
                 "ConversionActions2" => row["ConversionActions2"],
                 "ConversionActions3" => row["ConversionActions3"],
                 "ConversionActions4" => row["ConversionActions4"],
                 "ConversionActions5" => row["ConversionActions5"],
                 "ConversionActions6" => row["ConversionActions6"],
                 "ConversionActions7" => row["ConversionActions7"],
                 "ConversionActions8" => row["ConversionActions8"],
                 "ConversionActions9" => row["ConversionActions9"],
                 "ConversionActions10" => row["ConversionActions10"],
                 "ConversionActions11" => row["ConversionActions11"],
                 "ConversionActions12" => row["ConversionActions12"],
                 "ConversionActions13" => row["ConversionActions13"],
                 "ConversionActions14" => row["ConversionActions14"],
                 "ConversionActions15" => row["ConversionActions15"],
                 "ConversionActions16" => row["ConversionActions16"],
                 "ConversionActions17" => row["ConversionActions17"],
                 "ConversionActions18" => row["ConversionActions18"],
                 "ConversionActions19" => row["ConversionActions19"],
                 "ConversionActions20" => row["ConversionActions20"],
                 "ConversionActions21" => row["ConversionActions21"],
                 "ConversionActions22" => row["ConversionActions22"],
                 "ConversionActions23" => row["ConversionActions23"],
                 "ConversionActions24" => row["ConversionActions24"],
                 "ConversionActions25" => row["ConversionActions25"],
                 "ConversionActions26" => row["ConversionActions26"],
                 "ConversionActions27" => row["ConversionActions27"],
                 "ConversionActions28" => row["ConversionActions28"],
                 "ConversionActions29" => row["ConversionActions29"],
                 "ConversionActions30" => row["ConversionActions30"],
                 "ConversionActions31" => row["ConversionActions31"],
                 "ConversionActions32" => row["ConversionActions32"],
                 "ConversionActions33" => row["ConversionActions33"],
                 "ConversionActions34" => row["ConversionActions34"],
                 "ConversionActions35" => row["ConversionActions35"],
                 "ConversionActions36" => row["ConversionActions36"],
                 "ConversionActions37" => row["ConversionActions37"],
                 "ConversionActions38" => row["ConversionActions38"],
                 "ConversionActions39" => row["ConversionActions39"],
                 "ConversionActions40" => row["ConversionActions40"],
                 "ConversionActions41" => row["ConversionActions41"],
                 "ConversionActions42" => row["ConversionActions42"],
                 "ConversionActions43" => row["ConversionActions43"],
                 "ConversionActions44" => row["ConversionActions44"],
                 "ConversionActions45" => row["ConversionActions45"],
                 "ConversionActions46" => row["ConversionActions46"],
                 "ConversionActions47" => row["ConversionActions47"],
                 "ConversionActions48" => row["ConversionActions48"],
                 "ConversionActions49" => row["ConversionActions49"],
                 "ConversionActions50" => row["ConversionActions50"],
                 "ConversionActions51" => row["ConversionActions51"],
                 "ConversionActions52" => row["ConversionActions52"],
                 "ConversionActions53" => row["ConversionActions53"],
                 "ConversionActions54" => row["ConversionActions54"],
                 "ConversionActions55" => row["ConversionActions55"],
                 "ConversionActions56" => row["ConversionActions56"],
                 "ConversionActions57" => row["ConversionActions57"],
                 "ConversionActions58" => row["ConversionActions58"],
                 "ConversionActions59" => row["ConversionActions59"],
                 "ConversionActions60" => row["ConversionActions60"],
                 "ConversionActions61" => row["ConversionActions61"],
                 "ConversionActions62" => row["ConversionActions62"],
                 "ConversionActions63" => row["ConversionActions63"],
                 "ConversionActions64" => row["ConversionActions64"],
                 "ConversionActions65" => row["ConversionActions65"],
                 "ConversionActions66" => row["ConversionActions66"],
                 "ConversionActions67" => row["ConversionActions67"],
                 "ConversionActions68" => row["ConversionActions68"],
                 "ConversionActions69" => row["ConversionActions69"],
                 "ConversionActions70" => row["ConversionActions70"],
                 "ConversionActions71" => row["ConversionActions71"],
                 "ConversionActions72" => row["ConversionActions72"],
                 "ConversionActions73" => row["ConversionActions73"],
                 "ConversionActions74" => row["ConversionActions74"],
                 "ConversionActions75" => row["ConversionActions75"],
                 "ConversionActions76" => row["ConversionActions76"],
                 "ConversionActions77" => row["ConversionActions77"],
                 "ConversionActions78" => row["ConversionActions78"],
                 "ConversionActions79" => row["ConversionActions79"],
                 "ConversionActions80" => row["ConversionActions80"],
                 "ConversionActions81" => row["ConversionActions81"],
                 "ConversionActions82" => row["ConversionActions82"],
                 "ConversionActions83" => row["ConversionActions83"],
                 "ConversionActions84" => row["ConversionActions84"],
                 "ConversionActions85" => row["ConversionActions85"],
                 "ConversionActions86" => row["ConversionActions86"],
                 "ConversionActions87" => row["ConversionActions87"],
                 "ConversionActions88" => row["ConversionActions88"],
                 "ConversionActions89" => row["ConversionActions89"],
                 "ConversionActions90" => row["ConversionActions90"],
                 "ConversionActions91" => row["ConversionActions91"],
                 "ConversionActions92" => row["ConversionActions92"],
                 "ConversionActions93" => row["ConversionActions93"],
                 "ConversionActions94" => row["ConversionActions94"],
                 "ConversionActions95" => row["ConversionActions95"],
                 "ConversionActions96" => row["ConversionActions96"],
                 "ConversionActions97" => row["ConversionActions97"],
                 "ConversionActions98" => row["ConversionActions98"],
                 "ConversionActions99" => row["ConversionActions99"],

                 "promotionId" => row["promotionId"],
                 "isMonthlyBudget" => row["isMonthlyBudget"],
                 "Amount" => row["Amount"],
                 "Date" => row["Date"],
                 "CampaignId" => row["CampaignId"],
                 "CampaignName" => row["CampaignName"],
                 "del_flg" => 0,
               }
             end
  end

  # プロモーションデータ取得Powerpoint出力するため
  def promotionDataPowerPoint
    puts "全体のプロモーションデータ取得Powerpoint出力開始"
    #今日の日付
    #開始日 （前月の一日）
    startDate = Date.today.beginning_of_month.prev_month(12).to_s
    sDate = Date.today.beginning_of_month.prev_month(12)
    startMonth = Date.today.beginning_of_month.prev_month(12)

    #　今月の開始日
    thisMonthStartDate = Date.today.beginning_of_month

    #  本日
    todayDate = Date.today.to_s

    # 今月の開始日
    startDateOfThisMonth = Date.today.at_beginning_of_month

    #終了日（当日）
    endDate = Date.today.to_s
    eDate = Date.today
    endMonth = Date.today.beginning_of_month

    # 先月開始日
    lastMonthStartDate = Date.today.beginning_of_month.prev_month(1)
    # 先月終了日
    lastMonthEndDate = Date.today.end_of_month.prev_month(1)

    #BigQueryと接続
    bigquery = self.connectBigQuery

    # 媒体データリスト
    mediaArray = Array[
      I18n.t("promotionPowerPoint.Criteo"),
      I18n.t("promotionPowerPoint.GoogleAds"),
      I18n.t("promotionPowerPoint.FacebookAds"),
      I18n.t("promotionPowerPoint.LinkedInAds"),
      I18n.t("promotionPowerPoint.TwittertAds"),
      I18n.t("promotionPowerPoint.LINEAdsPlatform"),
      I18n.t("promotionPowerPoint.YahooSponsoredSearch"),
      I18n.t("promotionPowerPoint.YahooDisplayAdNetwork"),
    ]

    # 契約ステータス「契約中」のデータを取得
    contractStatusQuery = <<~SQL
      SELECT c.promotion_id 
      FROM #{APP_CONFIG["bigquery_dataset"]}.contract_status c 
                              WHERE c.contract_status IN (1,2) 
                              AND del_flg = 0
    SQL

    # プロモーションIdによりアラートデータ取得
    promotionList = bigquery.query <<~SQL
                                     SELECT
                                         DISTINCT databeat.PromotionId,
                                         databeat.PromotionName,
                                         databeat.ServiceAccountId,
                                         databeat.ServiceId,
                                         databeat.ServiceName,
                                         databeat.ServiceNameJA,                            
                                       FROM
                                         `wacul-databeat.databeat.databeat_attribute` databeat                           
                                       WHERE
                                         PromotionId NOT IN (#{contractStatusQuery}) order by PromotionName
                                   SQL

    groupPromotionData = promotionList.group_by { |x| x[:PromotionId] }.values

    # 媒体サービスデータ取得取得
    serviceNameTableSetting = bigquery.query <<~SQL
                                               SELECT
                                               *
                                               FROM
                                               `#{APP_CONFIG["bigquery_dataset"]}.service_name_table_setting` service_name_table_setting
                                                WHERE
                                                del_flg = 0
                                             SQL

    staticLength = 2
    lastStaticLength = groupPromotionData.length % 10
    default = 0
    selectQuery = ""
    promotionUsageAmount = []
    promotionsUsageAmount = []
    promotionLength = groupPromotionData.length
    promotionId = ""

    groupPromotionData.each do |data|
      default = default + 1
      data.each do |promotion|
        #不要文字を削除する
        #例：128-295-5819　⇒　1282955819
        serviceAccountId = promotion[:ServiceAccountId].tr "-", ""
        dataSetName = "#{promotion[:ServiceId]}_#{serviceAccountId}"
        serviceName =
          serviceNameTableSetting.find do |x|
            x[:service_id] == promotion[:ServiceId]
          end
        puts promotion[:PromotionName]
        promotionId = promotion[:PromotionId]
        table = "campaign"
        if serviceName.present? && serviceName[:table_name].present? &&
           %w[campaign_conversion campaign].include?(
             serviceName[:table_name],
           )
          table = serviceName[:table_name]
        end
        sql =
          self.getAllPromotionData promotion[:PromotionId],
                                   dataSetName,
                                   startDate,
                                   endDate,
                                   table,
                                   promotion[:ServiceName],
                                   promotion[:ServiceNameJA]

        if sql != ""
          selectQuery << "(#{sql}) UNION ALL "
        end
      end

      sqlQuery = " SELECT
                    SUM(ConversionValue) ConversionValue,
                    SUM(Amount) Amount,
                    SUM(Impressions) Impressions,
                    SUM(Clicks) Click,                    
                    SUM(AverageCpc) AverageCpc,                    
                    SUM(CostPerConversion) CostPerConversion,
                    Date,
                    CampaignId,
                    promotionId,
                    CampaignName,
                    Platform,
                    ServiceName,
                    ServiceNameJA
                  FROM (#{selectQuery.chomp! " UNION ALL "}) GROUP BY
                    Date,
                    CampaignId,
                    promotionId,
                    CampaignName,
                    Platform,
                    ServiceName,
                    ServiceNameJA
                  ORDER BY
                    Date,
                    promotionId,
                    CampaignId"

      usageAmount = bigquery.query sqlQuery

      # 月次サマリー（プロモーションと媒体により）シート3
      # 表示回数
      impression = 0
      # クリック数
      click = 0
      # クリック単価
      averageCpc = 0
      # ご利用金額
      amount = 0
      # コンバージョン(獲得件数）
      conversions = 0
      # コンバージョン単価(獲得単価)
      costPerConversion = 0
      # 媒体名
      serviceNameJA = ""
      # 月次サマリー
      oneMonthMedaiDataByPromotion = []
      arrayIndex = 0
      if usageAmount.length > 0
        #今月のデータを取得する
        oneMonthPromotionData = usageAmount.select { |data| data[:Date] >= thisMonthStartDate && data[:Date] <= eDate }
        if oneMonthPromotionData.length > 0
          while arrayIndex < mediaArray.length
            mediaByPromotionData = oneMonthPromotionData.select { |data| data[:ServiceNameJA] == mediaArray[arrayIndex] }
            if mediaByPromotionData.length > 0
              mediaByPromotionData.each do |data|
                # 表示回数
                impression = impression + data[:Impressions]
                # クリック数
                click = click + data[:Click]
                # 媒体
                if data[:ServiceNameJA] != nil
                  serviceNameJA = data[:ServiceNameJA]
                end
                # クリック単価
                if data[:AverageCpc] != nil
                  averageCpc = averageCpc + data[:AverageCpc]
                end
                # ご利用金額
                amount = amount + data[:Amount]
                # コンバージョン(獲得件数）
                conversions = conversions + data[:ConversionValue]
                # コンバージョン単価(獲得単価)
                if data[:CostPerConversion] != nil
                  costPerConversion = costPerConversion + data[:CostPerConversion]
                end
              end
              oneMonthMedaiDataByPromotion << {
                "PromotionId" => promotionId,
                "ServiceNameJA" => serviceNameJA,
                "Impressions" => impression,
                "Click" => click,
                "AverageCpc" => averageCpc,
                "Amount" => amount,
                "ConversionValue" => conversions,
                "CostPerConversion" => costPerConversion,
              }
              impression = 0
              click = 0
              amount = 0
              averageCpc = 0
              conversions = 0
              costPerConversion = 0
            end
            arrayIndex = arrayIndex + 1
          end
          arrayIndex = 0
        end
      end

      # 月次サマリー_検索広告_媒体別シート4,5
      # 表示回数
      impression = 0
      # クリック数
      click = 0
      # クリック単価
      averageCpc = 0
      # ご利用金額
      amount = 0
      # コンバージョン(獲得件数）
      conversions = 0
      # コンバージョン単価(獲得単価)
      costPerConversion = 0
      # 媒体名
      serviceNameJA = ""
      # 月次サマリー（一年の値）
      dataByPromotion = []
      arrayIndex = 0

      if usageAmount.length > 0
        while arrayIndex < mediaArray.length
          # プロモーションの媒体によりデータ取得
          mediaByPromotionData = usageAmount.select { |data| data[:ServiceNameJA] == mediaArray[arrayIndex] }
          if mediaByPromotionData.length > 0
            # 媒体により毎月のデータを取得
            while startMonth <= endMonth
              dateByPromotion = mediaByPromotionData.select { |data| data[:Date] >= startMonth && data[:Date] <= startMonth.end_of_month }
              if dateByPromotion.length > 0
                dateByPromotion.each do |data|
                  # 表示回数
                  impression = impression + data[:Impressions]
                  # クリック数
                  click = click + data[:Click]
                  # 媒体
                  if data[:ServiceNameJA] != nil
                    serviceNameJA = data[:ServiceNameJA]
                  end
                  # クリック単価
                  if data[:AverageCpc] != nil
                    averageCpc = averageCpc + data[:AverageCpc]
                  end
                  # ご利用金額
                  amount = amount + data[:Amount]
                  # コンバージョン(獲得件数）
                  conversions = conversions + data[:ConversionValue]
                  # コンバージョン単価(獲得単価)
                  if data[:CostPerConversion] != nil
                    costPerConversion = costPerConversion + data[:CostPerConversion]
                  end
                end
              end
              dataByPromotion << {
                "PromotionId" => promotionId,
                "ServiceNameJA" => serviceNameJA,
                "Impressions" => impression,
                "Click" => click,
                "AverageCpc" => averageCpc,
                "Amount" => amount,
                "ConversionValue" => conversions,
                "CostPerConversion" => costPerConversion,
                "Date" => startMonth,
              }
              impression = 0
              click = 0
              amount = 0
              averageCpc = 0
              conversions = 0
              costPerConversion = 0
              serviceNameJA = ""
              startMonth = startMonth.next_month
            end
          end
          arrayIndex = arrayIndex + 1
          startMonth = Date.today.beginning_of_month.prev_month(12)
        end
        arrayIndex = 0
      end

      # キャンペーン_先々月_先月比較 シート6,7,8,9,10
      # 表示回数
      impression = 0
      # クリック数
      click = 0
      # クリック単価
      averageCpc = 0
      # ご利用金額
      amount = 0
      # コンバージョン(獲得件数）
      conversions = 0
      # コンバージョン単価(獲得単価)
      costPerConversion = 0
      # 月次サマリー（一年の値）
      lastMonthDataByCampainName = []
      # 前月
      lastMonth = Date.today.beginning_of_month.prev_month(1)
      # 前々月
      lastLastMonth = Date.today.beginning_of_month.prev_month(2)
      # キャンペーンId
      campaignId = ""

      if usageAmount.length > 0
        while arrayIndex < mediaArray.length
          # プロモーションの媒体によりデータ取得
          mediaByPromotionData = usageAmount.select { |data| data[:ServiceNameJA] == mediaArray[arrayIndex] }
          if mediaByPromotionData.length > 0
            # 媒体により前々月のデータを取得
            while lastMonth >= lastLastMonth
              lastMonthDataPromotion = mediaByPromotionData.select { |data| data[:Date] >= lastMonth && data[:Date] <= lastMonth.end_of_month }
              if lastMonthDataPromotion.length > 0
                groupCampaignData = lastMonthDataPromotion.group_by { |x| x[:CampaignId] }.values
                if groupCampaignData.length > 0
                  groupCampaignData.each do |campaign|
                    if campaign.length > 0
                      campaign.each do |data|
                        # 表示回数
                        impression = impression + data[:Impressions]
                        # クリック数
                        click = click + data[:Click]
                        # クリック単価
                        if data[:AverageCpc] != nil
                          averageCpc = averageCpc + data[:AverageCpc]
                        end
                        # ご利用金額
                        amount = amount + data[:Amount]
                        # コンバージョン(獲得件数）
                        conversions = conversions + data[:ConversionValue]
                        # コンバージョン単価(獲得単価)
                        if data[:CostPerConversion] != nil
                          costPerConversion = costPerConversion + data[:CostPerConversion]
                        end
                      end
                    end
                    lastMonthDataByCampainName << {
                      "PromotionId" => campaign[0][:promotionId],
                      "CampaignId" => campaign[0][:CampaignId],
                      "CampaignName" => campaign[0][:CampaignName],
                      "ServiceNameJA" => campaign[0][:ServiceNameJA],
                      "Impressions" => impression,
                      "Click" => click,
                      "AverageCpc" => averageCpc,
                      "Amount" => amount,
                      "ConversionValue" => conversions,
                      "CostPerConversion" => costPerConversion,
                      "Date" => lastMonth,
                    }
                    impression = 0
                    click = 0
                    amount = 0
                    averageCpc = 0
                    conversions = 0
                    costPerConversion = 0
                  end
                end
              end
              lastMonth = lastMonth.prev_month
            end
            lastMonth = Date.today.beginning_of_month.prev_month(1)
          end
          arrayIndex = arrayIndex + 1
        end
        arrayIndex = 0
      end

      # ディスプレイ広告_Google‗プレイスメント_先月_0CV_多コストTOPシート15
      data.each do |promotion|
        #不要文字を削除する
        #例：128-295-5819　⇒　1282955819
        serviceAccountId = promotion[:ServiceAccountId].tr "-", ""
        dataSetName = "#{promotion[:ServiceId]}_#{serviceAccountId}"
        serviceName =
          serviceNameTableSetting.find do |x|
            x[:service_id] == promotion[:ServiceId]
          end
        table = "campaign"
        if serviceName.present? && serviceName[:table_name].present? &&
           %w[campaign_conversion campaign].include?(
             serviceName[:table_name],
           )
          table = serviceName[:table_name]
        end
        serviceNameJA = promotion[:ServiceNameJA]
        if serviceNameJA == I18n.t("promotionPowerPoint.GoogleAds")
          placementData = bigquery.query <<~SQL
                                           SELECT                                                 
                                             SUM(campaign.Cost) Amount,
                                             SUM(campaign.Conversions) ConversionValue,  
                                             SUM(campaign.Impressions) Impressions,
                                             SUM(campaign.Clicks) Click,                                                        
                                             SUM(campaign.AverageCpc) AverageCpc,                                                        
                                             SUM(campaign.CostPerConversion) CostPerConversion,
                                             placement.CampaignId,
                                             placement.CampaignName,
                                             placement.Placement Placements,
                                             placement.AdGroupName                                               
                                           FROM  
                                             `#{dataSetName}.placement` placement
                                                      JOIN
                                                        `#{dataSetName}.campaign` campaign 
                                                      ON
                                                        campaign.CampaignId = placement.CampaignId
                                                      WHERE 
                                                        campaign.Date BETWEEN "#{lastMonthStartDate}"
                                                      AND "#{lastMonthEndDate}"
                                                      AND placement.Date BETWEEN "#{lastMonthStartDate}"
                                                      AND "#{lastMonthEndDate}"
                                                      AND campaign.Conversions = 0
                                                      GROUP BY
                                                        placement.CampaignId,
                                                        placement.CampaignName,
                                                        Placements,
                                                        placement.AdGroupName
                                                      ORDER BY
                                                        Amount DESC limit 20
                                         SQL
        end
      end

      #ディスプレイ広告_Yahoo‗プレイスメント_先月_0CV_多コストTOP20 シート16
      data.each do |promotion|
        #不要文字を削除する
        #例：128-295-5819　⇒　1282955819
        serviceAccountId = promotion[:ServiceAccountId].tr "-", ""
        dataSetName = "#{promotion[:ServiceId]}_#{serviceAccountId}"
        serviceName =
          serviceNameTableSetting.find do |x|
            x[:service_id] == promotion[:ServiceId]
          end
        table = "campaign"
        if serviceName.present? && serviceName[:table_name].present? &&
           %w[campaign_conversion campaign].include?(
             serviceName[:table_name],
           )
          table = serviceName[:table_name]
        end
        serviceNameJA = promotion[:ServiceNameJA]
        if serviceNameJA == I18n.t("promotionPowerPoint.YahooDisplayAdNetwork")
          destinationURLData = bigquery.query <<~SQL
                                                SELECT                                                 
                                                  SUM(campaign.Cost) Amount,
                                                  SUM(campaign.Conversions) ConversionValue,  
                                                  SUM(campaign.Impressions) Impressions,
                                                  SUM(campaign.Clicks) Click,                                                        
                                                  SUM(campaign.AverageCpc) AverageCpc,                                                        
                                                  SUM(campaign.CostPerConversion) CostPerConversion,
                                                  ad.CampaignId,
                                                  ad.CampaignName,
                                                  ad.DisplayUrl,
                                                  ad.AdGroupName                                                                                                      
                                                FROM  
                                                  `#{dataSetName}.ad` ad
                                                      JOIN
                                                        `#{dataSetName}.campaign` campaign 
                                                      ON
                                                        campaign.CampaignId = ad.CampaignId
                                                      WHERE 
                                                        campaign.Date BETWEEN "#{lastMonthStartDate}"
                                                      AND "#{lastMonthEndDate}"
                                                      AND ad.Date BETWEEN "#{lastMonthStartDate}"
                                                      AND "#{lastMonthEndDate}"
                                                      AND campaign.Conversions = 0
                                                      GROUP BY
                                                        ad.CampaignId,
                                                        ad.CampaignName,
                                                        ad.DisplayUrl,
                                                        ad.AdGroupName
                                                      ORDER BY
                                                        Amount DESC limit 20
                                              SQL
        end
      end

      # ディスプレイ広告_Facebook‗配置先一覧 シート17
      # 表示回数
      impression = 0
      # クリック数
      click = 0
      # クリック単価
      averageCpc = 0
      # ご利用金額
      amount = 0
      # コンバージョン(獲得件数）
      conversions = 0
      # コンバージョン単価(獲得単価)
      costPerConversion = 0
      # 月次サマリー（一年の値）
      promotionDataByFacebook = []
      if usageAmount.length > 0
        # 媒体Facebookによりデータ取得
        mediaByPromotionData = usageAmount.select { |data| data[:ServiceNameJA] == I18n.t("promotionPowerPoint.FacebookAds") }
        if mediaByPromotionData.length > 0
          groupByPlatform = mediaByPromotionData.group_by { |x| x[:Platform] }.values
          if groupByPlatform.length > 0
            groupByPlatform.each do |platform|
              if platform.length > 0
                platform.each do |data|
                  # 表示回数
                  impression = impression + data[:Impressions]
                  # クリック数
                  click = click + data[:Click]
                  # クリック率
                  if data[:Ctr] != nil
                    ctr = ctr + data[:Ctr]
                  end
                  # クリック単価
                  if data[:AverageCpc] != nil
                    averageCpc = averageCpc + data[:AverageCpc]
                  end
                  # ご利用金額
                  amount = amount + data[:Amount]
                  # コンバージョン(獲得件数）
                  conversions = conversions + data[:ConversionValue]
                  # コンバージョン率(獲得率)
                  if data[:ConversionRate] != nil
                    conversionRate = conversionRate + data[:ConversionRate]
                  end
                  # コンバージョン単価(獲得単価)
                  if data[:CostPerConversion] != nil
                    costPerConversion = costPerConversion + data[:CostPerConversion]
                  end
                end
                promotionDataByFacebook << {
                  "PromotionId" => platform[0][:promotionId],
                  "Platform" => platform[0][:Platform],
                  "Impressions" => impression,
                  "Click" => click,
                  "AverageCpc" => averageCpc,
                  "Amount" => amount,
                  "ConversionValue" => conversions,
                  "CostPerConversion" => costPerConversion,
                }
                impression = 0
                click = 0
                amount = 0
                averageCpc = 0
                conversions = 0
                costPerConversion = 0
              end
            end
          end
        end
      end
      # promotionsUsageAmount << usageAmount
      selectQuery = ""
    end

    # # 月次サマリー（一年の値）シート2
    # # 表示回数
    # impression = 0
    # # クリック数
    # click = 0
    # # クリック率
    # ctr = 0
    # # クリック単価
    # averageCpc = 0
    # # ご利用金額
    # amount = 0
    # # コンバージョン(獲得件数）
    # conversions = 0
    # # コンバージョン率(獲得率)
    # conversionRate = 0
    # # コンバージョン単価(獲得単価)
    # costPerConversion = 0
    # # 月次サマリー（一年の値）
    # oneYearDataByPromotion = []
    # # 月次サマリー（一年の値）設設定
    # if promotionsUsageAmount.length > 0
    #   promotionsUsageAmount.each do |promotion|
    #     if promotion.length > 0
    #       while startMonth <= endMonth
    #         dateByPromotion = promotion.select {|data| data[:Date] >= startMonth && data[:Date] <= startMonth.end_of_month}
    #         if dateByPromotion.length > 0
    #           dateByPromotion.each do |data|
    #             impression = impression + data[:Impressions]
    #             click = click + data[:Click]
    #             if data[:Ctr] != nil
    #               ctr = ctr + data[:Ctr]
    #             end
    #             if data[:averageCpc] != nil
    #               averageCpc = averageCpc + data[:AverageCpc]
    #             end
    #             amount = amount + data[:Amount]
    #             conversions = conversions + data[:ConversionValue]
    #             if data[:ConversionRate] != nil
    #               conversionRate = conversionRate + data[:ConversionRate]
    #             end
    #             if data[:CostPerConversion] != nil
    #               costPerConversion = costPerConversion + data[:CostPerConversion]
    #             end
    #           end
    #         end
    #         oneYearDataByPromotion  << {
    #             'PromotionId' => promotion[0][:promotionId],
    #             'Impressions' => impression,
    #             'Click' => click,
    #             'Ctr' => ctr,
    #             'AverageCpc' => averageCpc,
    #             'Amount' => amount,
    #             'ConversionValue' => conversions,
    #             'ConversionRate' => conversionRate,
    #             'CostPerConversion' => costPerConversion,
    #             'Date' => startMonth,
    #           }
    #         impression = 0
    #         click = 0
    #         ctr = 0
    #         averageCpc = 0
    #         amount = 0
    #         conversions = 0
    #         conversionRate = 0
    #         costPerConversion = 0
    #         startMonth = startMonth.next_month
    #       end
    #       startMonth = Date.today.beginning_of_month.prev_month(12)
    #     end
    #   end
    # end
    puts "全体のプロモーションデータ取得Powerpoint出力終了"
  end

  # プロモーションデータ取得クエリ（Powerpoint出力）
  def getAllPromotionData(promotionId, dataSetName, startDate, endDate, tableName, serviceName, serviceNameJA)
    selectQuery = ""
    if tableName == "campaign"
      query = <<~SQL
        SELECT
          #{self.getConversionValueColumnPromotionList(tableName)},             
          '#{promotionId}' as promotionId,   
          '#{serviceName}' as ServiceName,
          '#{serviceNameJA}' as ServiceNameJA,
          Impressions As Impressions,      
          Clicks As Clicks, 
          Ctr As Ctr,
          AverageCpc As AverageCpc,
          ConversionRate As ConversionRate,
          CostPerConversion As CostPerConversion,
          Cost As Amount,
          Date As Date,
          CampaignId As CampaignId ,
          CampaignName As CampaignName, 
          Platform As Platform,             
        FROM
            `#{dataSetName}.#{tableName}` campaign
        WHERE
            Date BETWEEN '#{startDate}' AND '#{endDate}'            
            ORDER BY Date   
      SQL
      return query
    else
      query = <<~SQL
        SELECT
        0 AS ConversionValue,'' AS ConversionTypeName,#{(0..99).map { |i| "0 AS ConversionActions#{i}" }.join(",")},
        '#{promotionId}' as promotionId, 
        '#{serviceName}' as ServiceName,
        '#{serviceNameJA}' as ServiceNameJA,   
        Impressions As Impressions,
        Clicks As Clicks,   
        Ctr As Ctr,  
        AverageCpc As AverageCpc,
        ConversionRate As ConversionRate,
        CostPerConversion As CostPerConversion,
        Cost As Amount,        
        Date As Date,
        CampaignId As CampaignId,
        CampaignName As CampaignName,
        Platform As Platform,       
        FROM
            `#{dataSetName}.campaign` campaign
        WHERE
            Date BETWEEN '#{startDate}' AND '#{endDate}'           
            ORDER BY Date 
      SQL
      selectQuery << " (#{query}) UNION ALL "

      query = <<~SQL
        SELECT
          #{self.getConversionValueColumnPromotionList(tableName)},
          '#{promotionId}' as promotionId,  
          '#{serviceName}' as ServiceName,
          '#{serviceNameJA}' as ServiceNameJA,
          0 As Impressions, 
          0 As Ctr, 
          0 As Click, 
          0 As AverageCpc,   
          0 As ConversionRate, 
          0 As CostPerConversion,  
          0 AS Amount,
          Date As Date,
          CampaignId As CampaignId,
          CampaignName As CampaignName,
          "" As Platform,
        FROM
            `#{dataSetName}.campaign_conversion` campaign
        WHERE
            Date BETWEEN '#{startDate}' AND '#{endDate}'            
            ORDER BY Date 
      SQL
      selectQuery << "(#{query})"
      return selectQuery
    end
  end
end
