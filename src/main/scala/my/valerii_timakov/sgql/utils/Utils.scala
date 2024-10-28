package my.valerii_timakov.sgql.utils


trait AutoToString {
    override def toString: String = {
        this.getClass.getDeclaredFields.map { field =>
            field.setAccessible(true)
            s"${field.getName}=${field.get(this)}"
        }.mkString(s"${this.getClass.getSimpleName}(", ", ", ")")
    }
}
