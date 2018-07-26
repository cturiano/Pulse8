CREATE DATABASE Pulse8TestDB
GO

USE Pulse8TestDB
GO

CREATE TABLE Diagnosis
(
	DiagnosisID INT NOT NULL,
	DiagnosisDescription NVARCHAR(100) NOT NULL,
	PRIMARY KEY (DiagnosisID)
);

INSERT INTO dbo.Diagnosis ( DiagnosisID , DiagnosisDescription )
VALUES  ( 1 , 'Test Diagnosis 1' );

INSERT INTO dbo.Diagnosis ( DiagnosisID , DiagnosisDescription )
VALUES  ( 2 , 'Test Diagnosis 2' );

INSERT INTO dbo.Diagnosis ( DiagnosisID , DiagnosisDescription )
VALUES  ( 3 , 'Test Diagnosis 3' );

INSERT INTO dbo.Diagnosis ( DiagnosisID , DiagnosisDescription )
VALUES  ( 4 , 'Test Diagnosis 4' );

INSERT INTO dbo.Diagnosis ( DiagnosisID , DiagnosisDescription )
VALUES  ( 5 , 'Test Diagnosis 5' );

CREATE TABLE DiagnosisCategory
(
	DiagnosisCategoryID INT NOT NULL,
	CategoryDescription NVARCHAR(100) NOT NULL,
	CategoryScore INT NOT NULL,
	PRIMARY KEY (DiagnosisCategoryID)
);

INSERT INTO dbo.DiagnosisCategory ( DiagnosisCategoryID , CategoryDescription , CategoryScore )
VALUES  ( 1 , 'Category A', 10);

INSERT INTO dbo.DiagnosisCategory ( DiagnosisCategoryID , CategoryDescription , CategoryScore )
VALUES  ( 2 , 'Category B', 20);

INSERT INTO dbo.DiagnosisCategory ( DiagnosisCategoryID , CategoryDescription , CategoryScore )
VALUES  ( 3 , 'Category C', 30);

CREATE TABLE DiagnosisCategoryMap
(
	DiagnosisCategoryID INT NOT NULL,
	DiagnosisID INT NOT NULL
);

INSERT INTO dbo.DiagnosisCategoryMap ( DiagnosisCategoryID ,DiagnosisID )
VALUES  ( 1 , 1 );

INSERT INTO dbo.DiagnosisCategoryMap ( DiagnosisCategoryID ,DiagnosisID )
VALUES  ( 2 , 2 );

INSERT INTO dbo.DiagnosisCategoryMap ( DiagnosisCategoryID ,DiagnosisID )
VALUES  ( 3 , 3 );

INSERT INTO dbo.DiagnosisCategoryMap ( DiagnosisCategoryID ,DiagnosisID )
VALUES  ( 3 , 4 );

CREATE TABLE dbo.Member
(
	MemberID INT NOT NULL,
	FirstName NVARCHAR(50) NOT NULL, 
	LastName NVARCHAR(50) NOT NULL,
	PRIMARY KEY (MemberID)
);

INSERT INTO dbo.Member ( MemberID, FirstName, LastName )
VALUES  ( 1, 'John', 'Smith');

INSERT INTO dbo.Member ( MemberID, FirstName, LastName )
VALUES  ( 2, 'Jack', 'Smith');

INSERT INTO dbo.Member ( MemberID, FirstName, LastName )
VALUES  ( 3, 'Will', 'Smyth');

CREATE TABLE MemberDiagnosis
(
	MemberID INT NOT NULL,
	DiagnosisID INT NOT NULL
);

INSERT INTO dbo.MemberDiagnosis ( MemberID, DiagnosisID )
VALUES  ( 1, 2);

INSERT INTO dbo.MemberDiagnosis ( MemberID, DiagnosisID )
VALUES  ( 1, 4);

INSERT INTO dbo.MemberDiagnosis ( MemberID, DiagnosisID )
VALUES  ( 3, 3);

INSERT INTO dbo.MemberDiagnosis ( MemberID, DiagnosisID )
VALUES  ( 3, 4);



