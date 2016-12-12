Rate_PUF_2016 = LOAD 'projectfile/2016/Rate_PUF_2016.csv'
            USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
            AS (BusinessYear: int, StateCode: chararray, IssuerId: chararray, SourceName: chararray, VersionNum: chararray, ImportDate: chararray, IssuerId2: chararray, FederalTIN: chararray, RateEffectiveDate: chararray, RateExpirationDate: chararray, PlanId: chararray, RatingAreaId: chararray, Tobacco: chararray, Age: chararray, IndividualRate: int, IndividualTobaccoRate: int, Couple: int, PrimarySubscriberAndOneDependent: int, PrimarySubscriberAndTwoDependents: int, PrimarySubscriberAndThreeOrMoreDependents: int, CoupleAndOneDependent: int, CoupleAndTwoDependents: int, CoupleAndThreeOrMoreDependents: int, RowNumber: chararray);



R2016 = FOREACH Rate_PUF_2016 GENERATE BusinessYear, StateCode, PlanId, Tobacco, Age, IndividualRate, IndividualTobaccoRate;



grp_as = GROUP R2016 BY (StateCode, Age);
grp_as_rate = FOREACH grp_as GENERATE FLATTEN(group) as (StateCode, Age), R2016.IndividualRate as IndividualRates;
avg_as_rate = FOREACH grp_as_rate GENERATE Age, StateCode, AVG(IndividualRates) as avg;


STORE avg_as_rate INTO 'avg_as_rate' using PigStorage(',');

