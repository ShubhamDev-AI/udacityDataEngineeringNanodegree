
TRUNCATE TABLE
	public.music_store

;
SELECT
	*
	
FROM	
	music_store
	
;

INSERT INTO
    public.music_store(
         transaction_id
        ,customer_name
        ,cashier_name
        ,"year"
        ,albums_purchased
        
    )
    
VALUES(
	 4
	,'Alfred'
	,'Bob'
	,2019
	,ARRAY['Californication','Stadim Arcadium']
)

;

INSERT INTO
    public.music_store(
         transaction_id
        ,customer_name
        ,cashier_name
        ,"year"
        ,albums_purchased
        
    )
    
VALUES(
	 4
	,'Alfred'
	,'Bob'
	,2019
	,('{"Californication","Stadim Arcadium"}')
	
)