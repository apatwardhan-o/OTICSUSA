USE QualityShareData;
GO

IF OBJECT_ID('dbo.CMM_A25CH_Measurements', 'U') IS NOT NULL
    DROP TABLE dbo.CMM_A25CH_Measurements;
GO

CREATE TABLE CMM_A25CH_Measurements (
    PartType NVARCHAR(100),
    Model NVARCHAR(100),
    FilePath NVARCHAR(MAX),
    FileName NVARCHAR(500),
    FileCreatedAt DATETIME,
    Line# NVARCHAR(3),
    QShift NVARCHAR(2),
    Piece NVARCHAR(10),
    ProcessNo NVARCHAR(100),
    Cavity NVARCHAR(3),
    PosNo NVARCHAR(100),
    Item NVARCHAR(255),
    Element NVARCHAR(255),
    Nominal FLOAT,
    UpperLimit FLOAT,
    LowerLimit FLOAT,
    Actual FLOAT,
    Deviation FLOAT,
    Bar NVARCHAR(100),
    UL FLOAT,
    LL FLOAT,
    LoadTimestamp DATETIME DEFAULT GETDATE()
);
GO