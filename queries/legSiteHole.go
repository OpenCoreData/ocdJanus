package queries

var Sql_lsh string = "SELECT DISTINCT leg,site,hole from hole ORDER BY leg,site,hole"
var Sql_lsh5 string = "SELECT * FROM (SELECT DISTINCT leg,site,hole from hole ORDER BY leg,site,hole) where ROWNUM <= 5"
var Sql_inspectTest string = "SELECT * FROM ocd_chem_carb WHERE leg = 138 AND site = 844 AND hole = 'B'"
