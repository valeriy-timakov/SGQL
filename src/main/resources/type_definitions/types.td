
i.valerii_timakov.home_finances {
    MyInt: Integer(Integer)
    MyIntSub: MyInt
    users {
        MyArray: (UUID) [String]
        User: Object(UUID) {
            userName: String
            firstName: String
            lastName: String
            accounts: finances.Account+owner
        }
        U2: (Long) {
            abc: String
            dcVf: MyArray
            dcVfGb: MyArray
            fakeObj: {
                fakeField: String
                field2: MyArray
                field3: MyInt
                field14: MyInt
                field4: Object {
                    field5: MyArray
                    field6: MyArray
                    field7: String
                }
                field7: {
                    field8: inner.sdf.MyArray1
                    field9: String
                }
                field11: U3 {
                    field8_test: inner.sdf.MyArray2
                    field9: String
                }
                user: User
                user1: User {  }
            }
            fakeObj2: finances.Account {
                fakeField: String
                field2: MyArray
                field3: MyInt
                userX: $User
                something: MyIntSub
                something1: MyIntSub
            }
            fakeObj3: User {
                fakeField: String
                field2: MyArray
                field3: MyInt!
            }
            fakeObj4: User {
                fakeField: String!
                field2: MyArray
                field3: MyInt
            }
        }
        U3: U2 {
            vbn: $MyInt
            vbn2: MyInt
            vbn3: MyInt
        }
        inner {
            sdf{
                MyArray1!: Array(Long) [String],
                MyArray2!: Array(UUID) [String, DateTime],
            }
            MyArray2: sdf.MyArray1 [Date]
        }
        U4: U3 {
            yhn: inner.MyArray2!,
            acc: i.valerii_timakov.home_finances.finances.Account,
        }
        U5: U3 {
            yhn: inner.MyArray2,
            acc: i.valerii_timakov.home_finances.finances.Account,
        }
        U6: U3 {
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
            something: MyInt
            something1: MyInt
        }
        some.inner.super.type1.HiddenAccount: Array(UUID) [String]
    }
}