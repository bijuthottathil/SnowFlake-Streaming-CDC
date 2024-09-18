# SnowFlake-Streaming with Change Data Capture

![image](https://github.com/user-attachments/assets/a7694054-4000-4597-b9da-e3f69dbb4dc5)




![image](https://github.com/user-attachments/assets/72ef2e08-a4c3-47d5-a4fb-d9ccac7ceaba)


![image](https://github.com/user-attachments/assets/b81a82ab-bdaf-4f35-b60c-b2f9e7913205)


![image](https://github.com/user-attachments/assets/40172f2a-299a-4aca-a931-b8bf110e1207)


![image](https://github.com/user-attachments/assets/e1342aa5-2c00-4eeb-8234-44bb64d1a59e)


![image](https://github.com/user-attachments/assets/b31bedd0-ba90-4724-a19f-992c22d8c2ce)

![image](https://github.com/user-attachments/assets/e9304b89-51a7-4d22-9c6d-479460c9ebb3)


Still records did not come to final table. We need to consume the stream in to final table

![image](https://github.com/user-attachments/assets/37511d7a-3395-4145-8708-9862eb9a405a)


After consumption, stream is empty

![image](https://github.com/user-attachments/assets/a1cf7491-5072-4ca3-b080-a609dabe1489)

But values added to final table from stream


![image](https://github.com/user-attachments/assets/bae5e77e-e6ec-4cac-9397-4fac61edb8e8)


Updating data and checking changes reflecting in result table




![image](https://github.com/user-attachments/assets/eb98af1c-3d9e-4dae-a349-dbab975ba139)

You will see 2 entries. delete and insert

![image](https://github.com/user-attachments/assets/e3f8a4ea-a60e-4b51-9a65-6216c091c8a0)

To get update entry in final table, use merge statement like below

merge into SALES_FINAL_TABLE F      -- Target table to merge changes from source table
using SALES_STREAM S                -- Stream that has captured the changes
   on  f.id = s.id                 
when matched 
    and S.METADATA$ACTION ='INSERT'
    and S.METADATA$ISUPDATE ='TRUE'        -- Indicates the record has been updated 
    then update 
    set f.product = s.product,
        f.price = s.price,
        f.amount= s.amount,
        f.store_id=s.store_id;
You can see banana is updated with potato in final table using merge

![image](https://github.com/user-attachments/assets/7996396c-747c-4804-83fc-37611905f523)


Handling delete in stream

![image](https://github.com/user-attachments/assets/cb058918-c145-4cf8-877b-61b39ce5101d)

![image](https://github.com/user-attachments/assets/4d7a45d6-2ad1-4057-aad6-58ab0d0497c3)

![image](https://github.com/user-attachments/assets/7ca9b712-d05e-443d-b6f6-b6a84a0fc125)


Multiple operations together

![image](https://github.com/user-attachments/assets/7eca6e18-9988-4802-806e-e7f9af18c630)

# -- ******* Process UPDATE,INSERT & DELETE simultaneously  ********   
![image](https://github.com/user-attachments/assets/76bc8614-fdc6-434b-bb79-961dcd52538c)

Below code used to insert initial records in final table

-- Insert into final table
  insert into sales_final_table 
  select s.id ,
  s.product ,
  s.price ,
  s.amount ,
  s.store_id,st.location,st.employees from 
  sales_raw_staging s join store_table st on st.store_id= s.store_id

![image](https://github.com/user-attachments/assets/4363d2b6-1140-4f91-b6e0-1d4a283d145c)

![image](https://github.com/user-attachments/assets/612f28e3-ff1f-4d5a-b2be-51fff41b246f)

![image](https://github.com/user-attachments/assets/0c3e5f10-b684-4e58-89eb-1c7f24129c03)


So far no data

![image](https://github.com/user-attachments/assets/7c2a9011-17d5-4eae-bb6c-6be505cb14a4)




![image](https://github.com/user-attachments/assets/1ca7af3e-0c36-4bdc-9f1f-5a118571d0df)


Fixed from here

![image](https://github.com/user-attachments/assets/b1fd9bf4-5f57-48ac-9c78-2604822e3067)

![image](https://github.com/user-attachments/assets/d2898c78-26d6-4061-92a2-a2696e247d25)

![image](https://github.com/user-attachments/assets/9961a412-b66c-49df-aa2c-9e8bb8b6213b)

Doing 3 operations together

![image](https://github.com/user-attachments/assets/ff55c6e0-80ce-4ead-9caa-35d646c3c3db)

![image](https://github.com/user-attachments/assets/77d5c731-4cef-4def-8049-40702a348c37)


-- Merging all changes in single command

![image](https://github.com/user-attachments/assets/d8a24fd2-8440-4ca4-9159-c4d9196802f5)


-- Merging all changes in single command

merge into sales_final_table f using
(select stre.* , st.location ,st.employees from sales_stream stre join store_table st on st.store_id=stre.store_id) S
on f.id=s.id
when matched
and s.metadata$isupdate='TRUE' and s.metadata$action='INSERT'
then update
set  f.product=S.product ,f.price=S.price,f.amount=S.amount,f.store_id=S.store_id
when matched
and s.metadata$action='DELETE' and s.metadata$isupdate='FALSE'
then delete
when not matched
and S.metadata$action='INSERT' then
insert (id,product,price,amount,store_id,location,employees) values(S.id,S.product,S.price,S.amount,S.store_id,S.location,S.employees)


![image](https://github.com/user-attachments/assets/d6570ebb-a100-4852-a2dd-61ecc72c4fde)


![image](https://github.com/user-attachments/assets/34afe670-fd92-4357-8eeb-7537e1ec59b2)


------- Automatate the updates using tasks --

New task created the same queries used above

![image](https://github.com/user-attachments/assets/3031394e-a86e-4a07-af0d-f6d2eb7d2954)

To run task, execute below


![image](https://github.com/user-attachments/assets/fa7e9b4c-2494-4fd5-bedf-be126fcb4e1c)



![image](https://github.com/user-attachments/assets/7712765c-ec23-444f-ab2d-e6fe2676c97d)



![image](https://github.com/user-attachments/assets/4a433592-7e9f-4943-86a3-482a3f9db4fe)


![image](https://github.com/user-attachments/assets/59b0a986-cade-4c25-abcc-a7c8f36cd56a)



![image](https://github.com/user-attachments/assets/c55631ff-4a64-4117-8e84-97ef445561d1)


// Verify the history
select *
from table(information_schema.task_history())
order by name asc,scheduled_time desc;





![image](https://github.com/user-attachments/assets/1d78b7ae-6237-4223-a221-93226da5f7aa)





        










