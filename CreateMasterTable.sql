USE [QualityShareData]
GO

-- 1. Drop the table if it already exists so the script can be re-run
DROP TABLE IF EXISTS [dbo].[CMM_Measurements];
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CMM_Measurements](
    [ID] [int] IDENTITY(1,1) NOT NULL,       -- Auto-incrementing primary key (optional but recommended)
    [PartType] [nvarchar](50) NULL,
    [Model] [nvarchar](50) NULL,
    [FilePath] [nvarchar](400) NOT NULL,     -- Critical: Used to check for duplicates
    [FileName] [nvarchar](255) NULL,
    [FileCreatedAt] [datetime] NULL,         -- This will store your extracted/regex date
    [Line#] [nvarchar](50) NULL,
    [QShift] [nvarchar](20) NULL,
    [Piece] [nvarchar](50) NULL,
    [ProcessNo] [nvarchar](50) NULL,
    [Cavity] [nvarchar](50) NULL,
    
    -- Measurement Data
    [PosNo] [nvarchar](50) NULL,             -- Kept as string just in case
    [Item] [nvarchar](150) NULL,
    [Element] [nvarchar](150) NULL,
    [Nominal] [float] NULL,
    [UpperLimit] [float] NULL,               -- Calculated (Nominal + UL)
    [LowerLimit] [float] NULL,               -- Calculated (Nominal + LL)
    [Actual] [float] NULL,
    [Deviation] [float] NULL,
    [Bar] [nvarchar](100) NULL,              -- Visual bar representation from the file
    [UL] [float] NULL,                       -- Raw Upper Tolerance
    [LL] [float] NULL,                       -- Raw Lower Tolerance
    
    [UploadTimestamp] [datetime] DEFAULT GETDATE(), -- Tracks when the script actually ran
    
    CONSTRAINT [PK_CMM_Measurements] PRIMARY KEY CLUSTERED 
    (
        [ID] ASC
    )
) ON [PRIMARY]
GO

-- Create an index on FilePath to make the "Duplicate Check" fast
CREATE NONCLUSTERED INDEX [IX_CMM_Measurements_FilePath] ON [dbo].[CMM_Measurements]
(
    [FilePath] ASC
)
GO