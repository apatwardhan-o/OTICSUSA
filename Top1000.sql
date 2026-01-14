SELECT TOP (1000) [CreatedDate]
      ,[Model]
      ,[Line#]
      ,[Process]
      ,[Shift]
      ,[Piece]
      ,[Machine #]
      ,[FileName]
      ,[FullPath]
  FROM [QualityShareData].[dbo].[CMM_Measurements]


SELECT TOP 1000 *
FROM [QualityShareData].[dbo].[CMM_Measurements]
WHERE [Model] = '967K'
ORDER BY [CreatedDate] DESC;