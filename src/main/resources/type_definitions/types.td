
i.valerii_timakov.home_finances {
    MyInt: Integer(Integer)
    users {
        MyArray: Array(UUID) [String]
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
        inner {
            sdf{
                MyArray1: Array [String]
            }
            MyArray2: sdf.MyArray1(Long) [Date]
        }
        U4: U3(Long) {
            yhn: inner.MyArray2,
            acc: i.valerii_timakov.home_finances.finances.Account,
        }
    }
    finances {
        Account: Object(UUID) {
            name: String,
            description: String,
            external: Boolean,
            owner: i.valerii_timakov.home_finances.users.User
        }
        some.inner.super.type1.HiddenAccount: Array(UUID) [String]
    }
}