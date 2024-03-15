
i.valerii_timakov.home_finances {
    MyInt: Integer(Integer)
    users {
        MyArray: Array(UUID) [String]
        MyArray2: (Long) [Date]
        User: Object(UUID) {
            userName: String
            firstName: String
            lastName: String
        }
        U2: {
            abc: String
            dcf: MyArray
        }
        U3: U2 {
            vbn: MyInt
        }
        U4: U3(Long) {
            yhn: MyArray2,
            acc: finances.Account,
        }
    }
    finances {
        Account: Object(UUID) {
            name: String,
            description: String,
            external: Boolean,
            owner: users.User
        }
    }
}