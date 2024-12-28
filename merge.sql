DECLARE @ID uniqueidentifier

      --
      -- Load Tank Tickets
      --
      -- Matching on HaulerTankNumber

      IF OBJECT_ID('tempdb..#tempData') IS NOT NULL
            DROP TABLE #tempData

    SELECT
            tank.MerrickID,
            tank.TankName,
            9 DispositionCode,
            8 RunOrDispositionFlag,
            tank.ProductCode,
            tank.ProductType,
            case when tank.ProductType = 2 then 2
                  when tank.ProductType = 3 then 3
            end OilWaterRunFlag,
            17 DataSourceCode,   -- Scada
            1 ActualEstimatedFlag,
--          DateAdd(d, 1, cast(vols.RunTicketDate as date)) RecordDate,
            case
                  when day(cast(vols.RunTicketDate as date)) = 1 then  -- first of month
                        case
                              when cast(vols.RunTicketDate as time) < '07:00' then        -- before 7am set to previous day
                                    DateAdd(d, -1, cast(vols.RunTicketDate as date))
                              else
                                    cast(vols.RunTicketDate as date) -- use RunTicketDate
                        end
                  else
                        DateAdd(d, 1, cast(vols.RunTicketDate as date)) -- else use RunTicketDate
            end RecordDate,
            cast(vols.RunTicketDate as date) RunTicketDate,
            cast(vols.OpenDate as date) OpenDate,
            cast(vols.CloseDate as date) CloseDate,
            vols.TicketNumber,
            vols.OpenMeterReading OpenOdometer,
            vols.CloseMeterReading CloseOdometer,
            vols.APIGravity,
            vols.ObservedGravity,
            vols.BSWPercentage,
            vols.ObservedTemperature,
            vols.TopTemperature,
            vols.BottomTemperature,
            cast(vols.EstimatedBarrels as float) GrossBarrels,
            cast(vols.EstimatedNetBarrels as float) NetBarrels,
            cast(vols.TopFeet as int) TopFeet,
            cast(vols.TopInches as int) TopInches,
            cast(vols.TopFracNumer as int) TopFracNumer,
            cast(vols.TopFracDenom as int) TopFracDenom,
            cast(vols.BottomFeet as int) BottomFeet,
            cast(vols.BottomInches as int) BottomInches,
            cast(vols.BottomFracNumer as int) BottomFracNumer,
            cast(vols.BottomFracDenom as int) BottomFracDenom,
            223 Purchaser,
            case
                  when tank.HaulerBEID <> 0 then tank.HaulerBEID
                  when vols.TicketSubmitter = 'Trimac' then 1003
                  when vols.TicketSubmitter = 'Indeca' then 173
                  when vols.TicketSubmitter = 'Plains' then 93
                  else 0
            end Hauler,
            vols.SealOff,
            vols.SealOn,
            2 UseHaulerBarrelsFlag,
            2 DeleteFlag,
            2 CalculationStatusFlag,
            2 ApiVersion,
            vols.Record_ID,
            isnull(lst.Value, 50) SealType
      INTO #tempData
    FROM
                  [Staging].[DTS_Inbound_eTickets] vols
        INNER JOIN
                  [dbo].[TankTb] tank
        ON  
--                tank.HaulerTankNumber = vols.[TankNumber]
                  SUBSTRING(tank.HaulerTankNumber, PATINDEX('%[^0]%',tank.HaulerTankNumber), 4000) = SUBSTRING(vols.TankNumber, PATINDEX('%[^0]%',vols.TankNumber), 4000)
                  and vols.[TankNumber] <> ''
            LEFT JOIN
                  ListSealTypeTb lst
            on
                  Replace(vols.SealLocation, '"', '') = lst.Description
    WHERE
            vols.Record_Status = 0
        AND tank.ProductCode IN (2)
            -- ADDED 1/16/2024
            and vols.TicketType not in (4)
            and vols.TankNumber not in ('1', '2', '3')
            and cast(vols.EstimatedNetBarrels as float) > 0.000


    -- Merge/Upsert
    MERGE dbo.TankRunTicketTB AS [Target]
      USING (SELECT * FROM #tempData) AS [Source]
      ON [Target].MerrickID = [Source].MerrickID AND
--        [Target].RecordDate = [Source].RecordDate AND
        [Target].RecordDate between dateadd(d, -2, cast([Source].RecordDate as date)) and dateadd(d, 2, cast([Source].RecordDate as date)) AND
            [Target].RunTicketNumber = [Source].TicketNumber AND
            [Target].DeleteFlag <> 1
      WHEN MATCHED /*AND [Target].DataSourceCode IN (0, 2, 15, 17) */
            AND Target.NetBarrels <> Source.NetBarrels
            THEN
            UPDATE SET
                [Target].ProductCode = [Source].ProductCode,
                [Target].ProductType = [Source].ProductType,
                [Target].DispositionCode = [Source].DispositionCode,
                        [Target].RunOrDispositionFlag = [Source].RunOrDispositionFlag,
                        [Target].GrossBarrels = case when [Source].GrossBarrels = 0 then [Source].NetBarrels else [Source].GrossBarrels end,
                        [Target].NetBarrels = [Source].NetBarrels,
                        [Target].AllocatedOilBarrels = [Source].NetBarrels,
                        [Target].OpenFeet = [Source].TopFeet,
                        [Target].OpenInch = [Source].TopInches,
                        [Target].OpenQuarter = 4*(cast([Source].TopFracNumer as float)/cast([Source].TopFracDenom as float)),
                        [Target].TopOpenTotalInches = ([Source].TopFeet *12)+([Source].TopInches)+(cast([Source].TopFracNumer as float)/cast([Source].TopFracDenom as float)),
                        [Target].CloseFeet = [Source].BottomFeet,
                        [Target].CloseInch = [Source].BottomInches,
                        [Target].CloseQuarter = 4*(cast([Source].BottomFracNumer as float)/cast([Source].BottomFracDenom as float)),
                        [Target].TopCloseTotalInches = ([Source].BottomFeet *12)+([Source].BottomInches)+(cast([Source].BottomFracNumer as float)/cast([Source].BottomFracDenom as float)),
                        [Target].[RunCloseDate] = [Source].[RecordDate],
                        [Target].[OpenOdometer] = [Source].[OpenOdometer],
                        [Target].[CloseOdometer] = [Source].[CloseOdometer],
                        [Target].[OpenTemperature] = [Source].[TopTemperature],
                        [Target].[CloseTemperature] = [Source].[BottomTemperature],
                        [Target].[ObservedTemperature] = [Source].[ObservedTemperature],
                        [Target].[ConvertedGravity] = [Source].[APIGravity],
                        [Target].[ActualGravity] = [Source].[ObservedGravity],
                        [Target].[BSandW] = cast([Source].BSWPercentage as float),
                        [Target].[Purchaser] = [Source].[Purchaser], -- OEMI
                        [Target].[Hauler] = [Source].[Hauler],
                        [Target].[SealOff] = [Source].[SealOff],
                        [Target].[SealOn] = [Source].[SealOn],
                        [Target].[OilWaterRunFlag] = [Source].[OilWaterRunFlag],
                [Target].DataSourceCode = [Source].DataSourceCode,
                [Target].UserDateStamp = cast(getdate() as date),
                [Target].UserTimeStamp = cast(getdate() as time),
                [Target].UserID = 16,
                [Target].CalculationStatusFlag = [Source].CalculationStatusFlag,
                        [Target].ApiVersion = [Source].ApiVersion
      WHEN NOT MATCHED THEN
            INSERT
                  (
                MerrickID,
                ProductCode,
                ProductType,
                DispositionCode,
                DataSourceCode,
                        ActualEstimatedFlag,
                        RecordDate,
                        RunTicketNumber,
                        RunTicketDate,
                        RunCloseDate,
                        OpenOdometer,
                        CloseOdometer,
                        OpenFeet ,
                        OpenInch ,
                        OpenQuarter ,
                        TopOpenTotalInches ,
                        CloseFeet ,
                        CloseInch ,
                        CloseQuarter ,
                        TopCloseTotalInches ,
                        OpenTemperature,
                        CloseTemperature,
                        ObservedTemperature,
                        GrossBarrels,
                        NetBarrels,
                        RunOrDispositionFlag,
                        AllocatedOilBarrels,
                        ConvertedGravity,
                        ActualGravity,
                        BSandW,
                        Purchaser,
                        Hauler,
                        SealOff,
                        SealOn,
                        OilWaterRunFlag,
                        UseHaulerBarrelsFlag,
                        CalculationStatusFlag,
                        ApiVersion,
                UserDateStamp,
                UserTimeStamp,
                UserID
            )
            VALUES
                  (
                [Source].MerrickID,
                [Source].ProductCode,
                [Source].ProductType,
                [Source].DispositionCode,
                [Source].DataSourceCode,
                        [Source].ActualEstimatedFlag,
                        [Source].RecordDate,
                        [Source].TicketNumber,
                        [Source].RunTicketDate,
                        [Source].RecordDate,
                        [Source].OpenOdometer,
                        [Source].CloseOdometer,
                        [Source].TopFeet,
                        [Source].TopInches,
                        [Source].TopFracNumer,
                        ([Source].TopFeet *12)+([Source].TopInches)+(.25*[Source].TopFracNumer),
                        [Source].BottomFeet,
                        [Source].BottomInches,
                        [Source].BottomFracNumer,
                        ([Source].BottomFeet *12)+([Source].BottomInches)+(.25*[Source].BottomFracNumer),
                        [Source].TopTemperature,
                        [Source].BottomTemperature,
                        [Source].ObservedTemperature,
                        case when [Source].GrossBarrels = 0 then [Source].NetBarrels else [Source].GrossBarrels end,
                        [Source].NetBarrels,
                        [Source].RunOrDispositionFlag,
                        [Source].NetBarrels,
                        [Source].[APIGravity],
                        [Source].ObservedGravity,
                        cast([Source].BSWPercentage as float),
                        [Source].[Purchaser], -- OEMI
                        [Source].[Hauler],
                        [Source].SealOff,
                        [Source].SealOn,
                        [Source].OilWaterRunFlag,
                        [Source].UseHaulerBarrelsFlag,
                        [Source].CalculationStatusFlag,
                        2,
                cast(getdate() as date),
                cast(getdate() as time),
                16
            );
    ;

            SELECT TOP 1 @ID = Record_ID from #tempData order by Record_ID

            WHILE @ID IS NOT NULL
            BEGIN

            --
            -- Merge current record data (closing stock) into Tank Daily Record
            --
            MERGE dbo.TankDailyTb AS TARGET
            USING (select data.*,
                         Sales.RunVolume
                  from #tempData data
                  inner join
                        (
                        select
                              tix.MerrickID,
                              sum(tix.NetBarrels) RunVolume
                        from
                              [Staging].[DTS_Inbound_eTickets] vols
                        INNER JOIN
                              [dbo].[TankTb] tank
                        ON    
            --                tank.HaulerTankNumber = vols.[TankNumber]
                              SUBSTRING(tank.HaulerTankNumber, PATINDEX('%[^0]%',tank.HaulerTankNumber), 4000) = SUBSTRING(vols.TankNumber, PATINDEX('%[^0]%',vols.TankNumber), 4000)
                              and vols.[TankNumber] <> ''
                        inner join
                              TankRunTicketTb tix
                        on
                              tank.MerrickID = tix.MerrickID
                        where
                              tix.DeleteFlag <> 1
                              and case
                                    when day(cast(vols.RunTicketDate as date)) = 1 then  -- first of month
                                          case
                                                when cast(vols.RunTicketDate as time) < '07:00' then        -- before 7am set to previous day
                                                      DateAdd(d, -1, cast(vols.RunTicketDate as date))
                                                else
                                                      cast(vols.RunTicketDate as date) -- use RunTicketDate
                                          end
                                    else
                                          DateAdd(d, 1, cast(vols.RunTicketDate as date)) -- else use RunTicketDate
                              end = tix.RecordDate
                              and vols.Record_ID = @ID
                        Group by
                              tix.MerrickID
                        ) Sales
                  on
                        data.MerrickID = Sales.MerrickID
                  where Record_ID = @ID) AS SOURCE
            ON (TARGET.MerrickID = SOURCE.MerrickID
                        AND TARGET.RecordDate = cast([Source].RecordDate as date)
                        )
            --When records are matched, update the records if there is any change
            WHEN MATCHED AND TARGET.DataSourceCode IN (0, 15, 17, 19)  
                  THEN UPDATE
                  SET         
                        TARGET.AmbientTemperature = 60,
                        TARGET.BackgroundTaskFlag = 2,
                        TARGET.TopFeet = Source.BottomFeet,
                        TARGET.TopInch = Source.BottomInches,
                        TARGET.TopQuarter = Source.BottomFracNumer,
                        TARGET.TopTotalInches = ([Source].BottomFeet *12)+([Source].BottomInches)+(.25*[Source].BottomFracNumer),
                        TARGET.BottomFeet = 0,
                        TARGET.BottomInch = 0,
                        TARGET.BottomQuarter = 0,
                        TARGET.BottomTotalInches = 0,
                        TARGET.EndingOil = Staging.Get_Tank_Volume(Source.MerrickID, cast(Source.RecordDate as date), Source.BottomFeet, Source.BottomInches, Source.BottomFracNumer),
                        TARGET.EndingWater = Target.BeginningWater,
                        TARGET.ProductionOil = Staging.Get_Tank_Volume(Source.MerrickID, cast(Source.RecordDate as date), Source.BottomFeet, Source.BottomInches, Source.BottomFracNumer) - Target.BeginningOil + Source.RunVolume,
                        TARGET.TotalRunsOil = Source.RunVolume,
                        TARGET.ProductionWater= /*Target.EndingWater - Target.BeginningWater + */Target.TotalRunsWater,
                        TARGET.UserDateStamp = cast(getdate() AS date),
                        TARGET.UserTimeStamp = cast(getdate() AS time),
                        TARGET.UserID = 16,
--                      TARGET.UserID = case when Source.Contract_Date is not null then 1899
--                                  else 16
--                                  end,
                        TARGET.DataSourceCode = 17,  -- PI
                        TARGET.CalculationStatusFlag = 2
            ;

            SELECT TOP 1 @ID = Record_ID from #tempData WHERE Record_ID > @ID order by Record_ID
            IF @@ROWCOUNT = 0
            BREAK

      END


      SELECT TOP 1 @ID = Record_ID from #tempData order by Record_ID

      WHILE @ID IS NOT NULL
      BEGIN

            -- Update/Insert Seal Information
            -- Merge/Upsert
            MERGE dbo.SealTB AS [Target]
            USING (SELECT data.*
                  FROM #tempData data
                  LEFT JOIN
                        (select * from dbo.SealTB where ObjectType = 3) seals
                  ON
                        data.MerrickID = seals.ObjectID
                        and data.SealOn = seals.SealNumber
                  where
                        data.SealOn <> ''
                        and data.Record_ID = @ID
                        and seals.ObjectID IS NULL) AS [Source]
            ON [Target].ObjectID = [Source].MerrickID AND
                  [Target].ObjectType = 3 AND
                  [Target].DateOn = [Source].RecordDate AND
                  [Target].RunTicketNumberOn = [Source].TicketNumber
            WHEN MATCHED THEN
                  UPDATE SET
                        [Target].SealNumber = [Source].SealOn,
                        [Target].SealNumberRemoved = [Source].SealOff,
                        [Target].SealType = [Source].SealType,  -- PRIMARY SALES LINE
                        [Target].UserDateStamp = cast(getdate() as date),
                        [Target].UserTimeStamp = cast(getdate() as time),
                        [Target].UserID = 16
            WHEN NOT MATCHED THEN
                  INSERT
                        (
                              ObjectID,
                              ObjectType,
                              DateOn,
                              TimeOn,
                              DateOff,
                              SealType,
                              RunTicketNumberOn,
                              SealNumber,
                              SealNumberRemoved,
                              UserDateStamp,
                              UserTimeStamp,
                              UserID
                        )
                  VALUES
                        (
                              [Source].MerrickID,
                              3,  -- Tank
                              [Source].RecordDate,
                              '00:00',
                              '2999-01-01',
                              [Source].SealType, -- PRIMARY SALES LINE
                              [Source].TicketNumber,
                              [Source].SealOn,
                              [Source].SealOff,
                              cast(getdate() as date),
                              cast(getdate() as time),
                              16
                        );

                  -- Update/Insert Seal Information for Seal Removed
                  -- Merge/Upsert
                  MERGE dbo.SealTB AS [Target]
                  USING (SELECT * FROM #tempData where SealOn <> ''  and Record_ID = @ID) AS [Source]
                  ON [Target].ObjectID = [Source].MerrickID AND
                        [Target].ObjectType = 3 AND
                        [Target].SealNumber = [Source].SealOff
                  WHEN MATCHED THEN
                        UPDATE SET
                              [Target].OnFlag = 2,
                              [Target].UserDateStamp = cast(getdate() as date),
                              [Target].UserTimeStamp = cast(getdate() as time),
                              [Target].UserID = 16
                              ;

            SELECT TOP 1 @ID = Record_ID from #tempData WHERE Record_ID > @ID order by Record_ID
            IF @@ROWCOUNT = 0
            BREAK

      END

            --Processed, set Record_Status = 1 (processed)
            UPDATE VOLS
                  SET VOLS.[Record_Status] = 1
            FROM
                        Staging.[DTS_Inbound_eTickets] VOLS
                  INNER JOIN
                        (SELECT * from #tempData) AS XXX
                  ON    VOLS.Record_ID = XXX.Record_ID
            WHERE
                  vols.Record_Status = 0


      IF OBJECT_ID('tempdb..#tempData') IS NOT NULL
            DROP TABLE #tempData


      --
      -- Load Tank Tickets using Mapping table (Staging.DTS_Indeca_Mappings)
      --

      IF OBJECT_ID('tempdb..#temp2Data') IS NOT NULL
            DROP TABLE #temp2Data

    SELECT
            DISTINCT
            tank.MerrickID,
            tank.TankName,
            9 DispositionCode,
            8 RunOrDispositionFlag,
            tank.ProductCode,
            tank.ProductType,
            case when tank.ProductType = 2 then 2
                  when tank.ProductType = 3 then 3
            end OilWaterRunFlag,
            17 DataSourceCode,   -- Scada
            1 ActualEstimatedFlag,
--          DateAdd(d, 1, cast(vols.RunTicketDate as date)) RecordDate,
            case
                  when tank.StateID = 30 then         -- if New Mexico, check 1st of month tickets before 7am
                        case
                              when day(cast(vols.RunTicketDate as date)) = 1 then  -- first of month
                                    case
                                          when cast(vols.RunTicketDate as time) < '07:00' then        -- before 7am set to previous day
                                                DateAdd(d, -1, cast(vols.RunTicketDate as date))
                                          else
                                                DateAdd(d, 1, cast(vols.RunTicketDate as date)) -- else set to 2nd of month
                                    end
                              else
                                    cast(vols.RunTicketDate as date)    -- else use RunTicketDate
                        end
                  -- non New Mexico tickets just use RunTicketDate
                  else
                        case
                              when day(cast(vols.RunTicketDate as date)) = 1 then  -- first of month
                                    case
                                          when cast(vols.RunTicketDate as time) < '07:00' then        -- before 7am set to previous day
                                                DateAdd(d, -1, cast(vols.RunTicketDate as date))
                                          else
                                                cast(vols.RunTicketDate as date) -- use RunTicketDate
                                    end
                              else
                                    DateAdd(d, 1, cast(vols.RunTicketDate as date)) -- else use RunTicketDate
                        end
            end RecordDate,
            cast(vols.RunTicketDate as date) RunTicketDate,
            cast(vols.OpenDate as date) OpenDate,
            cast(vols.CloseDate as date) CloseDate,
            vols.TicketNumber,
            vols.OpenMeterReading OpenOdometer,
            vols.CloseMeterReading CloseOdometer,
            vols.APIGravity,
            vols.ObservedGravity,
            vols.BSWPercentage,
            vols.ObservedTemperature,
            vols.TopTemperature,
            vols.BottomTemperature,
            cast(vols.EstimatedBarrels as float) GrossBarrels,
            cast(vols.EstimatedNetBarrels as float) NetBarrels,
            cast(vols.TopFeet as int) TopFeet,
            cast(vols.TopInches as int) TopInches,
            cast(vols.TopFracNumer as int) TopFracNumer,
            cast(vols.TopFracDenom as int) TopFracDenom,
            cast(vols.BottomFeet as int) BottomFeet,
            cast(vols.BottomInches as int) BottomInches,
            cast(vols.BottomFracNumer as int) BottomFracNumer,
            cast(vols.BottomFracDenom as int) BottomFracDenom,
            223 Purchaser,
            case
                  when tank.HaulerBEID <> 0 then tank.HaulerBEID
                  when vols.TicketSubmitter = 'Trimac' then 1003
                  when vols.TicketSubmitter = 'Indeca' then 173
                  when vols.TicketSubmitter = 'Plains' then 93
                  else 0
            end Hauler,
            vols.SealOff,
            vols.SealOn,
            2 UseHaulerBarrelsFlag,
            2 DeleteFlag,
            2 CalculationStatusFlag,
            2 ApiVersion,
            vols.Record_ID,
            isnull(lst.Value, 50) SealType
      INTO #temp2Data
    FROM
            [Staging].[DTS_Inbound_eTickets] vols
      inner join
            Staging.DTS_Indeca_Mappings map
      on
            map.LeaseNumber = vols.LeaseNumber
            and SUBSTRING(map.ID , PATINDEX('%[^0]%',map.ID ), 4000) = SUBSTRING(vols.TankNumber, PATINDEX('%[^0]%',vols.TankNumber), 4000)
--          and map.ID = vols.TankNumber
            and map.TankorMeter = 'Tank'
      inner join
            TankTb tank
      on
            map.MerrickID = tank.MerrickID
      LEFT JOIN
            ListSealTypeTb lst
      on
            Replace(vols.SealLocation, '"', '') = lst.Description
    WHERE
            vols.Record_Status = 0
        AND tank.ProductCode IN (2)
            -- ADDED 1/16/2024
            and vols.TicketType not in (4)
            and vols.TankNumber not in ('1', '2', '3')


    -- Merge/Upsert
    MERGE dbo.TankRunTicketTB AS [Target]
      USING (SELECT * FROM #temp2Data) AS [Source]
      ON [Target].MerrickID = [Source].MerrickID AND
        [Target].RecordDate between dateadd(d, -2, cast([Source].RecordDate as date)) and dateadd(d, 2, cast([Source].RecordDate as date)) AND
            [Target].RunTicketNumber = [Source].TicketNumber AND
            [Target].DeleteFlag <> 1
      WHEN MATCHED /*AND [Target].DataSourceCode IN (0, 2, 15, 17)*/ THEN
            UPDATE SET
                [Target].ProductCode = [Source].ProductCode,
                [Target].ProductType = [Source].ProductType,
                [Target].DispositionCode = [Source].DispositionCode,
                        [Target].RunOrDispositionFlag = [Source].RunOrDispositionFlag,
                        [Target].GrossBarrels = case when [Source].GrossBarrels = 0.00 then [Source].NetBarrels else [Source].GrossBarrels end,
                        [Target].NetBarrels = [Source].NetBarrels,
                        [Target].AllocatedOilBarrels = [Source].NetBarrels,
                        [Target].OpenFeet = [Source].TopFeet,
                        [Target].OpenInch = [Source].TopInches,
                        [Target].OpenQuarter = 4*(cast([Source].TopFracNumer as float)/cast([Source].TopFracDenom as float)),
                        [Target].TopOpenTotalInches = ([Source].TopFeet *12)+([Source].TopInches)+(cast([Source].TopFracNumer as float)/cast([Source].TopFracDenom as float)),
                        [Target].CloseFeet = [Source].BottomFeet,
                        [Target].CloseInch = [Source].BottomInches,
                        [Target].CloseQuarter = 4*(cast([Source].BottomFracNumer as float)/cast([Source].BottomFracDenom as float)),
                        [Target].TopCloseTotalInches = ([Source].BottomFeet *12)+([Source].BottomInches)+(cast([Source].BottomFracNumer as float)/cast([Source].BottomFracDenom as float)),
                        [Target].[RunCloseDate] = [Source].[RecordDate],
                        [Target].[OpenOdometer] = [Source].[OpenOdometer],
                        [Target].[CloseOdometer] = [Source].[CloseOdometer],
                        [Target].[OpenTemperature] = [Source].[TopTemperature],
                        [Target].[CloseTemperature] = [Source].[BottomTemperature],
                        [Target].[ObservedTemperature] = [Source].[ObservedTemperature],
                        [Target].[ConvertedGravity] = [Source].[APIGravity],
                        [Target].[ActualGravity] = [Source].[ObservedGravity],
                        [Target].[BSandW] = cast([Source].BSWPercentage as float),
                        [Target].[Purchaser] = [Source].[Purchaser], -- OEMI
                        [Target].[Hauler] = [Source].[Hauler],
                        [Target].[SealOff] = [Source].[SealOff],
                        [Target].[SealOn] = [Source].[SealOn],
                        [Target].[OilWaterRunFlag] = [Source].[OilWaterRunFlag],
                [Target].DataSourceCode = [Source].DataSourceCode,
                [Target].UserDateStamp = cast(getdate() as date),
                [Target].UserTimeStamp = cast(getdate() as time),
                [Target].UserID = 16,
                [Target].CalculationStatusFlag = [Source].CalculationStatusFlag,
                        [Target].ApiVersion = [Source].ApiVersion
      WHEN NOT MATCHED THEN
            INSERT
                  (
                MerrickID,
                ProductCode,
                ProductType,
                DispositionCode,
                DataSourceCode,
                        ActualEstimatedFlag,
                        RecordDate,
                        RunTicketNumber,
                        RunTicketDate,
                        RunCloseDate,
                        OpenFeet ,
                        OpenInch ,
                        OpenQuarter ,
                        TopOpenTotalInches ,
                        CloseFeet ,
                        CloseInch ,
                        CloseQuarter ,
                        TopCloseTotalInches ,
                        OpenOdometer,
                        CloseOdometer,
                        OpenTemperature,
                        CloseTemperature,
                        ObservedTemperature,
                        GrossBarrels,
                        NetBarrels,
                        RunOrDispositionFlag,
                        AllocatedOilBarrels,
                        ConvertedGravity,
                        ActualGravity,
                        BSandW,
                        Purchaser,
                        Hauler,
                        SealOff,
                        SealOn,
                        OilWaterRunFlag,
                        UseHaulerBarrelsFlag,
                        CalculationStatusFlag,
                        ApiVersion,
                UserDateStamp,
                UserTimeStamp,
                UserID
            )
            VALUES
                  (
                [Source].MerrickID,
                [Source].ProductCode,
                [Source].ProductType,
                [Source].DispositionCode,
                [Source].DataSourceCode,
                        [Source].ActualEstimatedFlag,
                        [Source].RecordDate,
                        [Source].TicketNumber,
                        [Source].RunTicketDate,
                        [Source].RecordDate,
                        [Source].TopFeet,
                        [Source].TopInches,
                        [Source].TopFracNumer,
                        ([Source].TopFeet *12)+([Source].TopInches)+(.25*[Source].TopFracNumer),
                        [Source].BottomFeet,
                        [Source].BottomInches,
                        [Source].BottomFracNumer,
                        ([Source].BottomFeet *12)+([Source].BottomInches)+(.25*[Source].BottomFracNumer),
                        [Source].OpenOdometer,
                        [Source].CloseOdometer,
                        [Source].TopTemperature,
                        [Source].BottomTemperature,
                        [Source].ObservedTemperature,
                        case when [Source].GrossBarrels = 0.00 then [Source].NetBarrels else [Source].GrossBarrels end,
                        [Source].NetBarrels,
                        [Source].RunOrDispositionFlag,
                        [Source].NetBarrels,
                        [Source].[APIGravity],
                        [Source].ObservedGravity,
                        cast([Source].BSWPercentage as float),
                        [Source].[Purchaser], -- OEMI
                        [Source].[Hauler],
                        [Source].SealOff,
                        [Source].SealOn,
                        [Source].OilWaterRunFlag,
                        [Source].UseHaulerBarrelsFlag,
                        [Source].CalculationStatusFlag,
                        2,
                cast(getdate() as date),
                cast(getdate() as time),
                16
            );
    ;


      SELECT TOP 1 @ID = Record_ID from #temp2Data order by Record_ID

      WHILE @ID IS NOT NULL
      BEGIN

            --
            -- Merge current record data (closing stock) into Tank Daily Record
            --
            MERGE dbo.TankDailyTb AS TARGET
            USING (select data.*,
                         Sales.RunVolume
                  from #temp2Data data
                  inner join
                        (
                        select
                              tix.MerrickID,
                              sum(tix.NetBarrels) RunVolume
                        from
                              [Staging].[DTS_Inbound_eTickets] vols
                        inner join
                              Staging.DTS_Indeca_Mappings map
                        on
                              map.LeaseNumber = vols.LeaseNumber
                              and SUBSTRING(map.ID , PATINDEX('%[^0]%',map.ID ), 4000) = SUBSTRING(vols.TankNumber, PATINDEX('%[^0]%',vols.TankNumber), 4000)
                        --          and map.ID = vols.TankNumber
                              and map.TankorMeter = 'Tank'
                        inner join
                              TankRunTicketTb tix
                        on
                              map.MerrickID = tix.MerrickID
                        where
                              tix.DeleteFlag <> 1
                              and case
                                    when day(cast(vols.RunTicketDate as date)) = 1 then  -- first of month
                                          case
                                                when cast(vols.RunTicketDate as time) < '07:00' then        -- before 7am set to previous day
                                                      DateAdd(d, -1, cast(vols.RunTicketDate as date))
                                                else
                                                      cast(vols.RunTicketDate as date) -- use RunTicketDate
                                          end
                                    else
                                          DateAdd(d, 1, cast(vols.RunTicketDate as date)) -- else use RunTicketDate
                              end = tix.RecordDate
                              and vols.Record_ID = @ID
                        Group by
                              tix.MerrickID
                        ) Sales
                  on
                        data.MerrickID = Sales.MerrickID
                  where Record_ID = @ID) AS SOURCE
            ON (TARGET.MerrickID = SOURCE.MerrickID
                        AND TARGET.RecordDate = cast([Source].RecordDate as date)
                        )
            --When records are matched, update the records if there is any change
            WHEN MATCHED AND TARGET.DataSourceCode IN (0, 15, 17, 19)  
                  THEN UPDATE
                  SET         
                        TARGET.AmbientTemperature = 60,
                        TARGET.BackgroundTaskFlag = 2,
                        TARGET.TopFeet = Source.BottomFeet,
                        TARGET.TopInch = Source.BottomInches,
                        TARGET.TopQuarter = Source.BottomFracNumer,
                        TARGET.TopTotalInches = ([Source].BottomFeet *12)+([Source].BottomInches)+(.25*[Source].BottomFracNumer),
                        TARGET.BottomFeet = 0,
                        TARGET.BottomInch = 0,
                        TARGET.BottomQuarter = 0,
                        TARGET.BottomTotalInches = 0,
                        TARGET.EndingOil = Staging.Get_Tank_Volume(Source.MerrickID, cast(Source.RecordDate as date), Source.BottomFeet, Source.BottomInches, Source.BottomFracNumer),
                        TARGET.EndingWater = Target.BeginningWater,
                        TARGET.ProductionOil = Staging.Get_Tank_Volume(Source.MerrickID, cast(Source.RecordDate as date), Source.BottomFeet, Source.BottomInches, Source.BottomFracNumer) - Target.BeginningOil + Source.RunVolume,
                        TARGET.TotalRunsOil = Source.RunVolume,
                        TARGET.ProductionWater= /*Target.EndingWater - Target.BeginningWater + */Target.TotalRunsWater,
                        TARGET.UserDateStamp = cast(getdate() AS date),
                        TARGET.UserTimeStamp = cast(getdate() AS time),
                        TARGET.UserID = 16,
                        TARGET.DataSourceCode = 17,  -- PI
                        TARGET.CalculationStatusFlag = 2
            ;

            SELECT TOP 1 @ID = Record_ID from #temp2Data WHERE Record_ID > @ID order by Record_ID
            IF @@ROWCOUNT = 0
            BREAK

      END



      SELECT TOP 1 @ID = Record_ID from #temp2Data order by Record_ID

      WHILE @ID IS NOT NULL
      BEGIN

            -- Update/Insert Seal Information
            -- Merge/Upsert
            MERGE dbo.SealTB AS [Target]
            USING (
      --    SELECT * FROM #temp2Data where SealOn not in ('', '0')
                  SELECT data.*
                        FROM #temp2Data data
                        LEFT JOIN
                              (select * from dbo.SealTB where ObjectType = 3) seals
                        ON
                              data.MerrickID = seals.ObjectID
                              and data.SealOn = seals.SealNumber
                        where
                              data.SealOn <> ''
                              and seals.ObjectID IS NULL
                              and data.Record_ID = @ID
                  ) AS [Source]
            ON [Target].ObjectID = [Source].MerrickID AND
                  [Target].ObjectType = 3 AND
                  [Target].DateOn = [Source].RecordDate AND
                  [Target].RunTicketNumberOn = [Source].TicketNumber
            WHEN MATCHED THEN
                  UPDATE SET
                        [Target].SealNumber = [Source].SealOn,
                        [Target].SealNumberRemoved = [Source].SealOff,
                        [Target].SealType = [Source].SealType,  -- PRIMARY SALES LINE
                        [Target].UserDateStamp = cast(getdate() as date),
                        [Target].UserTimeStamp = cast(getdate() as time),
                        [Target].UserID = 16
            WHEN NOT MATCHED THEN
                  INSERT
                        (
                              ObjectID,
                              ObjectType,
                              DateOn,
                              TimeOn,
                              DateOff,
                              SealType,
                              RunTicketNumberOn,
                              SealNumber,
                              SealNumberRemoved,
                              UserDateStamp,
                              UserTimeStamp,
                              UserID
                        )
                  VALUES
                        (
                              [Source].MerrickID,
                              3,  -- Tank
                              [Source].RecordDate,
                              '00:00',
                              '2999-01-01',
                              [Source].SealType, -- PRIMARY SALES LINE
                              [Source].TicketNumber,
                              [Source].SealOn,
                              [Source].SealOff,
                              cast(getdate() as date),
                              cast(getdate() as time),
                              16
                        );


            -- Update/Insert Seal Information for Seal Removed
            -- Merge/Upsert
            MERGE dbo.SealTB AS [Target]
            USING (SELECT * FROM #temp2Data where SealOn <> '' and Record_ID = @ID) AS [Source]
            ON [Target].ObjectID = [Source].MerrickID AND
                  [Target].ObjectType = 3 AND
                  [Target].SealNumber = [Source].SealOff
            WHEN MATCHED THEN
                  UPDATE SET
                        [Target].OnFlag = 2,
                        [Target].UserDateStamp = cast(getdate() as date),
                        [Target].UserTimeStamp = cast(getdate() as time),
                        [Target].UserID = 16
                        ;

            SELECT TOP 1 @ID = Record_ID from #temp2Data WHERE Record_ID > @ID order by Record_ID
            IF @@ROWCOUNT = 0
            BREAK

      END



    --Processed, set Record_Status = 1 (processed)
    UPDATE VOLS
            SET VOLS.[Record_Status] = 1
      FROM
                  Staging.[DTS_Inbound_eTickets] VOLS
        INNER JOIN
                  (SELECT * from #temp2Data) AS XXX
        ON  VOLS.Record_ID = XXX.Record_ID
      WHERE
            vols.Record_Status = 0


      IF OBJECT_ID('tempdb..#temp2Data') IS NOT NULL
            DROP TABLE #temp2Data
