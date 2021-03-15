with sub as (
    select * from db.annual 
    union 
    select * from db.monthly 
    union
    select * from db.prior1
    union 
    select * from db.prior2
)
INSERT into taBle    output_db.output_table    
select * from sub
order by category,month,rnk