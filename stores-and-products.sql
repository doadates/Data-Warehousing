CREATE TABLE Country (
  CountryID int NOT NULL,
  Name varchar(255) NOT NULL,
  PRIMARY KEY (CountryID)
);

CREATE TABLE Region (
  RegionID int NOT NULL ,
  CountryID int NOT NULL,
  Name varchar(255) NOT NULL,
  PRIMARY KEY (RegionID)
);

CREATE TABLE City (
  CityID int NOT NULL ,
  RegionID int NOT NULL,
  Name varchar(255) NOT NULL,
  PRIMARY KEY (CityID)
);

CREATE TABLE Shop (
  ShopID int NOT NULL ,
  CityID int NOT NULL,
  Name varchar(255) NOT NULL,
  PRIMARY KEY (ShopID)
);

CREATE TABLE Article (
  ArticleID int NOT NULL ,
  ProductGroupID int NOT NULL,
  Name varchar(255) NOT NULL,
  Price double precision NOT NULL,
  PRIMARY KEY (ArticleID)
);

CREATE TABLE ProductGroup (
  ProductGroupID int NOT NULL ,
  ProductFamilyID int NOT NULL,
  Name varchar(255) NOT NULL,
  PRIMARY KEY (ProductGroupID)
);

CREATE TABLE ProductFamily (
  ProductFamilyID int NOT NULL ,
  ProductCategoryID int NOT NULL,
  Name varchar(255) NOT NULL,
  PRIMARY KEY (ProductFamilyID)
);

CREATE TABLE ProductCategory (
  ProductCategoryID int NOT NULL ,
  Name varchar(255) NOT NULL,
  PRIMARY KEY (ProductCategoryID)
);

ALTER TABLE Shop ADD CONSTRAINT ShopID_fk_1 FOREIGN KEY (CityID) REFERENCES City (CityID);
ALTER TABLE City ADD CONSTRAINT CityID_fk_1 FOREIGN KEY (RegionID) REFERENCES Region (RegionID);
ALTER TABLE Region ADD CONSTRAINT RegionID_fk_1 FOREIGN KEY (CountryID) REFERENCES Country (CountryID);

ALTER TABLE Article ADD CONSTRAINT ArticleID_fk_1 FOREIGN KEY (ProductGroupID) REFERENCES ProductGroup (ProductGroupID);
ALTER TABLE ProductGroup ADD CONSTRAINT ProductGroupID_fk_1 FOREIGN KEY (ProductFamilyID) REFERENCES ProductFamily (ProductFamilyID);
ALTER TABLE ProductFamily ADD CONSTRAINT ProductFamilyID_fk_1 FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory (ProductCategoryID);



INSERT INTO Country (CountryID, Name) VALUES (1, 'Deutschland');

INSERT INTO Region (RegionID, CountryID, Name) VALUES (1, 1, 'Baden-Württemberg');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (2, 1, 'Bayern');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (3, 1, 'Berlin');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (4, 1, 'Brandenburg');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (5, 1, 'Bremen');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (6, 1, 'Hamburg');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (7, 1, 'Hessen');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (8, 1, 'Mecklenburg-Vorpommern');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (9, 1, 'Niedersachsen');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (10, 1, 'Nordrhein-Westfalen');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (11, 1, 'Rheinland-Pfalz');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (12, 1, 'Saarland');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (13, 1, 'Sachsen');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (14, 1, 'Sachsen-Anhalt');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (15, 1, 'Schleswig-Holstein');
INSERT INTO Region (RegionID, CountryID, Name) VALUES (16, 1, 'Thüringen');

INSERT INTO City (CityID, RegionID, Name) VALUES (1, 1, 'Stuttgart');
INSERT INTO City (CityID, RegionID, Name) VALUES (2, 2, 'München');
INSERT INTO City (CityID, RegionID, Name) VALUES (3, 3, 'Berlin');
INSERT INTO City (CityID, RegionID, Name) VALUES (4, 4, 'Potsdam');
INSERT INTO City (CityID, RegionID, Name) VALUES (5, 5, 'Bremen');
INSERT INTO City (CityID, RegionID, Name) VALUES (6, 6, 'Hamburg');
INSERT INTO City (CityID, RegionID, Name) VALUES (7, 7, 'Wiesbaden');
INSERT INTO City (CityID, RegionID, Name) VALUES (8, 8, 'Schwerin');
INSERT INTO City (CityID, RegionID, Name) VALUES (9, 9, 'Hannover');
INSERT INTO City (CityID, RegionID, Name) VALUES (10, 10, 'Düsseldorf');
INSERT INTO City (CityID, RegionID, Name) VALUES (11, 11, 'Mainz');
INSERT INTO City (CityID, RegionID, Name) VALUES (12, 12, 'Saarbrücken');
INSERT INTO City (CityID, RegionID, Name) VALUES (13, 13, 'Dresden');
INSERT INTO City (CityID, RegionID, Name) VALUES (14, 14, 'Magdeburg');
INSERT INTO City (CityID, RegionID, Name) VALUES (15, 15, 'Kiel');
INSERT INTO City (CityID, RegionID, Name) VALUES (16, 16, 'Erfurt');

INSERT INTO Shop (ShopID, CityID, Name) VALUES (1, 1, 'Superstore Stuttgart');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (2, 2, 'Superstore München');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (3, 3, 'Superstore Berlin');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (4, 4, 'Superstore Potsdam');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (5, 5, 'Superstore Bremen');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (6, 6, 'Superstore Hamburg');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (7, 7, 'Superstore Wiesbaden');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (8, 8, 'Superstore Schwerin');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (9, 9, 'Superstore Hannover');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (10, 10, 'Superstore Düsseldorf');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (11, 11, 'Superstore Mainz');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (12, 12, 'Superstore Saarbrücken');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (13, 13, 'Superstore Dresden');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (14, 14, 'Superstore Magdeburg');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (15, 15, 'Superstore Kiel');
INSERT INTO Shop (ShopID, CityID, Name) VALUES (16, 16, 'Superstore Erfurt');

INSERT INTO ProductCategory (ProductCategoryID, Name) VALUES (1, 'Heimelektronik');
INSERT INTO ProductCategory (ProductCategoryID, Name) VALUES (2, 'Großgeräte');

INSERT INTO ProductFamily (ProductFamilyID, ProductCategoryID, Name) VALUES (1, 1, 'Video');
INSERT INTO ProductFamily (ProductFamilyID, ProductCategoryID, Name) VALUES (2, 1, 'Audio');
INSERT INTO ProductFamily (ProductFamilyID, ProductCategoryID, Name) VALUES (3, 2, 'Haushaltsgeräte');
INSERT INTO ProductFamily (ProductFamilyID, ProductCategoryID, Name) VALUES (4, 2, 'Gartengeräte');

INSERT INTO ProductGroup (ProductGroupID, ProductFamilyID, Name) VALUES (1, 1, 'HD-Rekorder');
INSERT INTO ProductGroup (ProductGroupID, ProductFamilyID, Name) VALUES (2, 1, 'Camcorder');
INSERT INTO ProductGroup (ProductGroupID, ProductFamilyID, Name) VALUES (3, 2, 'CD-Player');
INSERT INTO ProductGroup (ProductGroupID, ProductFamilyID, Name) VALUES (4, 2, 'Plattenspieler');
INSERT INTO ProductGroup (ProductGroupID, ProductFamilyID, Name) VALUES (5, 3, 'Waschmaschienen');
INSERT INTO ProductGroup (ProductGroupID, ProductFamilyID, Name) VALUES (6, 3, 'Trockner');
INSERT INTO ProductGroup (ProductGroupID, ProductFamilyID, Name) VALUES (7, 4, 'Rasenmäher');
INSERT INTO ProductGroup (ProductGroupID, ProductFamilyID, Name) VALUES (8, 4, 'Vertikutierer');

INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (1,1,'Pioneer DVR-550HX',469.989990234375);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (2,1,'LG RH-T 298',409.989990234375);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (3,1,'Samsung DVD-SR275',79.98999786376953);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (4,1,'BenQ DE350P',269.989990234375);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (5,2,'Panasonic HDC-SD707',779.989990234375);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (6,2,'Sony HDR-CX115',398.989990234375);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (7,2,'Kodak Zx3 Playsport',129.99000549316406);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (8,2,'Toshiba Camileo S20',99.98999786376953);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (9,3,'Onkyo DX-7355',139.99000549316406);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (10,3,'Yamaha CDX-497',239.99000549316406);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (11,3,'Sony CDP-XE370',140.89999389648438);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (12,3,'Denon DCD-710AE',369.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (13,4,'Sony PS-LX 300 USB',138.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (14,4,'Technics SL-1210 MK5',599.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (15,4,'Jaytec DJT-20',89.9000015258789);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (16,4,'Numark TT1650',148.99000549316406);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (17,5,'AEG Öko-Lavamat Öko Plus 1400',599.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (18,5,'Bosch WAE 2834P',499.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (19,5,'Miele Softtronic W 3241 WPS',798.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (20,5,'Bauknecht WA Sensitive 36 DI',349.8999938964844);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (21,6,'AEG Öko Lavatherm 59850 Sensidry',999.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (22,6,'Bauknecht TK Care 6B',279.9800109863281);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (23,6,'Miele Softtronic T 7744 C',749.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (24,6,'Bosch WTE 84301',449.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (25,7,'Wolf-Garten 2.34 E',149.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (26,7,'Bosch Rotak 43 LI (Modell 2009)',399.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (27,7,'Einhell RPM 56 S-MS',299.989990234375);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (28,7,'Gardena PowerMax 42 E',199.99000549316406);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (29,8,'Wolf-Garten Campus 1000 V',89.9000015258789);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (30,8,'Güde GV 2400 SL',249.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (31,8,'Brill 28 VE/RL',139.0);
INSERT INTO ARTICLE (ARTICLEID,PRODUCTGROUPID,NAME,PRICE) VALUES (32,8,'Gardena CS Vertikutier-Boy (3395-20)',49.900001525878906);
