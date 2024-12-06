DELIMITER $$

CREATE PROCEDURE UpdateAndSplitMDPR()
BEGIN
    -- Drop temporary table if exists
    DROP TEMPORARY TABLE IF EXISTS UpdatedWeatherData;

    -- Create the temporary table with calculated PRCP
    CREATE TEMPORARY TABLE UpdatedWeatherData AS
    SELECT 
        STATION, 
        DATE, 
        DAPR, 
        MDPR, 
        SNOW, 
        SNWD, 
        ROUND(
            CASE 
                WHEN DAPR IS NOT NULL AND DAPR > 0 THEN MDPR / DAPR
            END, 2
        ) AS PRCP
    FROM data_test
    WHERE DAPR > 1 AND DAPR IS NOT NULL AND MDPR IS NOT NULL;

    -- Store MAX DAPR to a variable
    SET @max_dapr = (SELECT MAX(DAPR) FROM UpdatedWeatherData);

    INSERT INTO data_test (STATION, DATE, DAPR, MDPR, PRCP, SNOW, SNWD)
	-- CTE making numeric table for backdating
    WITH RECURSIVE Backdating AS (
        SELECT 0 AS i
        UNION ALL
        SELECT i + 1
        FROM Backdating
        WHERE i + 1 < @max_dapr
    -- SELECT 
    --     TOP (@max_dapr) ROW_NUMBER() OVER (ORDER BY object_id) AS i 
    -- FROM sys.objects;
    
    ), 
    -- Grabs the SNOWD amount of the date before the DAPR begins
    PreviousSNWD AS (
        SELECT 
            ud.STATION,
            ud.DATE,
            ud.DAPR,
            ud.MDPR,
			ud.PRCP,
            ud.SNOW,
            ud.SNWD AS SNWD_current,
            (
                SELECT COALESCE(dt.SNWD, 0)  -- Treat NULL as 0
                FROM data_test dt
                WHERE dt.STATION = ud.STATION
                AND dt.DATE < ud.DATE
                ORDER BY dt.DATE DESC
                LIMIT 1
            ) AS SNWD_previous,
            b.i
        FROM UpdatedWeatherData ud
        JOIN Backdating b ON b.i < ud.DAPR
    )    
    SELECT 
        STATION,
        DATE_SUB(DATE, INTERVAL i DAY) AS DATE,
        NULL AS DAPR,
        NULL AS MDPR,
        PRCP,
        SNOW,
        -- Calculates the snowd using incrementation
        SNWD_current - ((SNWD_current - SNWD_previous) / DAPR) * i AS SNWD
    FROM PreviousSNWD;

    -- Delete the original row to prevent duplication (only the row where DAPR > 1)
    DELETE FROM data_test
    WHERE (STATION, DATE) IN (
        SELECT STATION, DATE FROM UpdatedWeatherData
    ) AND DAPR > 1;
    -- DELETE data_test
    -- FROM data_test
    --     FULL OUTER JOIN UpdatedWeatherData 
    --     ON data_test.STATION = UpdatedWeatherData.STATION AND data_test.DATE = UpdatedWeatherData.DATE
    -- WHERE UpdatedWeatherData.DAPR > 1;

    -- Drop the temporary tables
    DROP TEMPORARY TABLE UpdatedWeatherData;
END$$

DELIMITER ;