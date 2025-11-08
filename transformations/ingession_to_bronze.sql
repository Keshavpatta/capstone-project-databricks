
create streaming table accounts as 
select *, current_date() as ingestion_date from stream read_files("/Volumes/capstone_project/default/input_files/accounts/",format=>"json",multiLine => true);

create streaming table customers as 
select *, current_date() as ingestion_date from stream read_files("/Volumes/capstone_project/default/input_files/customer/",format=>"csv");

create streaming table transactions as 
select *, current_date() as ingestion_date from stream read_files("/Volumes/capstone_project/default/input_files/transaction/",format=>"csv");

create streaming table branch as 
select *, current_date() as ingestion_date from stream read_files("/Volumes/capstone_project/default/input_files/branch/",format=>"json",multiLine => true);

create streaming table loans as 
select *, current_date() as ingestion_date from stream read_files("/Volumes/capstone_project/default/input_files/loan/",format=>"json")
