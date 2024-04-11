
i.valerii_timakov.home_finances {
    MyInt: Integer(Integer)
    users {
        MyArray: (UUID) [String]
        User: Object(UUID) {
            userName: String
            firstName: String
            lastName: String
            accounts: Array [finances.Account+owner]
        }
        U2: {
            abc: String
            dcf: MyArray
        }
        U3: U2 {
            vbn: MyInt
            vbn2: MyInt
            vbn3: MyInt
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