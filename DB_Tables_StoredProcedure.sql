CREATE TABLE linklion2.dataset2 (
  indDS int(11) NOT NULL AUTO_INCREMENT,
  name varchar(200) DEFAULT NULL,
  PRIMARY KEY (indDS)
);

CREATE TABLE linklion2.uri2 (
  indURI int(11) NOT NULL AUTO_INCREMENT,
  uri varchar(900) DEFAULT NULL,
  indexDataset int(11) DEFAULT NULL,
  countDType int(11) DEFAULT NULL,
  PRIMARY KEY (indURI)
  );

DELIMITER //
CREATE DEFINER=`root`@`localhost` PROCEDURE `ADD_DB`(IN _subj varchar(900), _datasetEndPoint varchar(900))
BEGIN
DECLARE indexDS int(11) DEFAULT 0;
DECLARE cDtypes int(11) DEFAULT 0;
    
Select indDS INTO indexDS from linklion2.dataset2 where name = _datasetEndPoint;

IF (indexDS < 1) Then
Begin
	insert into linklion2.dataset2(name) values (_datasetEndPoint);
	COMMIT;
    set indexDS = LAST_INSERT_ID();
end;
End if;
#Select max(indDS) INTO indexDS from linklion2.dataset2;
Select sum(countDType) into cDtypes from linklion2.uri2 where 
uri = _subj and indexDataset = indexDS;
if(cDtypes IS NOT NULL) then
Begin
	Update linklion2.uri2 set countDType = (cDtypes + 1) 
		where uri = _subj and indexDataset = indexDS;
End;
else
Begin
	Insert into linklion2.uri2(uri,indexDataset,countDType) values (_subj, indexDS, 1);
End;
End if;
COMMIT;
END
//

CREATE DEFINER=`root`@`localhost` PROCEDURE `ADD_DB_S`(IN _subj varchar(900), _datasetEndPoint varchar(900), cDTypes int(11))
BEGIN
DECLARE indexDS int(11) DEFAULT 0;
    
Select indDS INTO indexDS from linklion2.dataset2 where name = _datasetEndPoint;

IF (indexDS < 1) Then
Begin
	insert into linklion2.dataset2(name) values (_datasetEndPoint);
	COMMIT;
    set indexDS = LAST_INSERT_ID();
end;
End if;
#Select max(indDS) INTO indexDS from linklion2.dataset2;
if(cDtypes IS NOT NULL) then
Begin
	Insert into linklion2.uri2(uri,indexDataset,countDType) values (_subj, indexDS, cDTypes);
End;
End if;
COMMIT;
END
//

CREATE DEFINER=`root`@`localhost` PROCEDURE `SearchTables`(in _uri varchar(6000))
BEGIN
	DECLARE v_finished INTEGER DEFAULT 0;
	DECLARE v_table varchar(600) DEFAULT "";
    DECLARE v_n varchar(600) DEFAULT "";
    
    DEClARE uri_cursor CURSOR FOR 
		SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES  WHERE table_schema = 'linklion2' and TABLE_NAME like 'datatypes_%';

	DECLARE CONTINUE HANDLER 
        FOR NOT FOUND SET v_finished = 1;
    
    drop temporary table if exists tmp;
    
    create temporary table tmp (
	dtypes int,
	dataset varchar(6000) DEFAULT NULL
	) engine=memory;
    
	OPEN uri_cursor;
	get_uri: LOOP	 
		FETCH uri_cursor INTO v_table;

		IF v_finished = 1 THEN 
			LEAVE get_uri;
		END IF;
		
        
        SET @sql_text1 = concat('Insert into tmp(dtypes, dataset) Select count(o) as dType, o from ',v_table
        , ' where s=\'',_uri,'\' group by o');

        PREPARE stmt1 FROM @sql_text1;
		EXECUTE stmt1;
		DEALLOCATE PREPARE stmt1;
		 
	 END LOOP get_uri;

	CLOSE uri_cursor;
	
    Select sum(dtypes) as dataTypes, dataset from tmp group by dataset;
END
