SET default_parallel 5;
DEFINE FORMAT_DT org.apache.pig.piggybank.evaluation.datetime.FORMAT_DT();
/*
EventType:
      0 - view_banner
      1 - click_banner
      2 - open_app
*/

RAW_DATA = LOAD '$INPUT_PATH'
USING JsonLoader('bannerId:int, appId:int, userId:int, timestamp:long, eventType:int');

PARSE_TIMESTAMP_TO_DATE = FOREACH RAW_DATA GENERATE
                           bannerId,
                           appId,
                           userId,
                           FORMAT_DT('yyyy-MM-dd', ToDate(timestamp)) as date,
                           eventType;
GROUP_EVENTS = GROUP PARSE_TIMESTAMP_TO_DATE BY (
                date,
                bannerId,
                appId);

CALCULATE_METRICS = FOREACH GROUP_EVENTS{
                    distinctUsers = distinct PARSE_TIMESTAMP_TO_DATE.userId;
                    bannerViews = filter PARSE_TIMESTAMP_TO_DATE by eventType == 0;
                    distinctBannerViewUsers = distinct bannerViews.userId;
                    bannerClicks = filter PARSE_TIMESTAMP_TO_DATE by eventType == 1;
                    distinctBannerClickUsers = distinct bannerClicks.userId;
                    bannerOpenApp = filter PARSE_TIMESTAMP_TO_DATE by eventType == 2;
                    distinctBannerOpenAppUsers = distinct bannerOpenApp.userId;
                    generate flatten(group),
                             COUNT(distinctUsers),
                             COUNT(bannerViews),
                             COUNT(distinctBannerViewUsers),
                             COUNT(distinctBannerClickUsers),
                             COUNT(distinctBannerOpenAppUsers),
                            (double)COUNT(distinctBannerClickUsers) / (double)COUNT(bannerViews),
                            (double)COUNT(distinctBannerOpenAppUsers) / (double)COUNT(bannerViews);
                    };

GET_NAMES = FOREACH CALCULATE_METRICS GENERATE
                $0 as date,
                $1 as bannerId,
                $2 as appId,
                $3 as distinctUsers,
                $4 as bannerViews,
                $5 as distinctBannerViewUsers,
                $6 as distinctBannerClickUsers,
                $7 as distinctBannerOpenAppUsers,
                $8 as ctr,
                $9 as itr;

ORDERED = ORDER GET_NAMES BY $0 PARALLEL 1;

STORE ORDERED INTO '$OUTPUT_PATH'
            USING org.apache.pig.piggybank.storage.CSVExcelStorage(
            '\t', 'YES_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

