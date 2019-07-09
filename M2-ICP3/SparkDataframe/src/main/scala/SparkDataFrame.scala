import org.apache.spark.sql.SparkSession


object SparkDataFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()


    import spark.implicits._

    val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
    val df1 = departmentsWithEmployeesSeq1.toDF()


    val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
    val df2 = departmentsWithEmployeesSeq2.toDF()


    df1.show()
    df2.show()
    val unionDF = df1.union(df2)
    unionDF.show()

  }
  // Create the case classes for our domain
  case class Department(id: String, name: String)
  case class Employee(firstName: String, lastName: String, email: String, salary: Int)
  case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])

  // Create the Departments
  val department1 = new Department("123456", "Computer Science")
  val department2 = new Department("789012", "Mechanical Engineering")
  val department3 = new Department("345678", "Theater and Drama")
  val department4 = new Department("901234", "Indoor Recreation")

  // Create the Employees
  val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
  val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
  val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
  val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)

  // Create the DepartmentWithEmployees instances from Departments and Employees
  val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
  val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee3, employee4))
  val departmentWithEmployees3 = new DepartmentWithEmployees(department3, Seq(employee1, employee4))
  val departmentWithEmployees4 = new DepartmentWithEmployees(department4, Seq(employee2, employee3))



}
