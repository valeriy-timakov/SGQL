i.valerii_timakov.home_finances {
	Party: (UUID) {
		fullName: String
	}

	UserPreferences: (UUID){
		locale: String
	}

	User: (UUID) {
		name: String 
		party: Party
	}

	SystemUser: User {
		systemUserId: UUID    
		preferences: UserPreferences
	}

	ContragentUser: User {
		owner: SystemUser
	}

	Currency: (UUID) {
		owner: SystemUser
		code: String
		name: String
		symbol: String
		fractionDigitsCount: Integer
	}

	Account: (UUID) {
		name: String
		currency: Currency
		description: String
		owner: User
	}

	OperationName: (UUID) {
		name: String
		owner: SystemUser
	}

	Operation: (UUID) {
		name: OperationName
		accountFrom: Account
		accountTo: Account
		amountFrom: Long
		amountTo: Long,
		description: String
		operationDate: DateTime
		addDate: DateTime
	}

	CurrencyExchange: (UUID) {
		sellOperation: Operation
		byOperation: Operation
	}

	OperationDetail: (UUID) {
		operation: Operation,
		product: Product,
		price: Long,
		quantity: Long,
		unit: MeasureUnit
	} 

	Category: (UUID) {
		name: String,
		description: String
		superCategory: Category
		created: DateTime
		owner: SystemUser
	}

	Producer: (UUID) {
		name: String
		owner: SystemUser
	}

	Product: (UUID) {
		name: String,
		category: Category,
		description: String
		producer: Producer
		size: String
	}

	PackedProduct: Product {
		packSize: Integer
		packUnit: MeasureUnit
	}


	MeasureUnit: (UUID) {
		name: String,
		shortName: String,
		owner: SystemUser
	}

	BaseMeasureUnit: MeasureUnit {}

	MultypliedMeasureUnit: MeasureUnit{
		scale: Float
		base: BaseMeasureUnit
	}
}

