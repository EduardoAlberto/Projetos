select * from dbo.tb_atores;
select * from dbo.tb_votos;


select * from dbo.tb_atores a 
inner join dbo.tb_votos b on a.ID01 = b.tconst 
or a.ID02 = b.tconst ; 