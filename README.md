# multidbrequest

## packages

- model
- postgres
- __//TODO:__ mongo

### model

model is a package that contains the imitation of the database and async requests to it.

### postgres

postgres is a package that contains the postgres database and async requests to it.

also you can find migration file here for 3 dbs

#### two main methods

 > getAllUsers(pool map[string]*sqlx.DB) ([]FullUser, error)
 
    return only users from databases

 > getAllUsersWithAllFields(pool map[string]*sqlx.DB) ([]FullUser, error)
     
    return users with all fields from databases