package test;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Create Spark Session to create connection to Spark
		final SparkSession sparkSession = SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[5]")
				.getOrCreate();
		// Please note that you will need to add a config property "spark.sql.warehouse.dir" with value as directory path in 
		// SparkSession builder if your csv file location is different from application directory as shown below -
		// final SparkSession sparkSession = SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[5]")
		// 				.config("spark.sql.warehouse.dir","/opt/data/").getOrCreate();

		// Get DataFrameReader using SparkSession
		final DataFrameReader dataFrameReader = sparkSession.read();
		// Set header option to true to specify that first row in file contains
		// name of columns
		dataFrameReader.option("header", "true");
//		final Dataset<Row> csvDataFrame = dataFrameReader.csv("C:\\all_data.csv").limit(200);
		final Dataset<Row> csvDataFrame = dataFrameReader.csv("C:\\all_data.csv");

		// Print Schema to see column names, types and other metadata
		csvDataFrame.printSchema();

//		 Create view and execute query to convert types as, by default, all columns have string types
		csvDataFrame.createOrReplaceTempView("PROFECO");
		final Dataset<Row> profecoData = sparkSession
					.sql( "SELECT producto, presentacion, marca, categoria, catalogo, CAST(precio AS float) AS precio, CAST(fechaRegistro AS date) AS fechaRegistro,"
						+ " cadenaComercial, giro, nombreComercial, direccion, estado, municipio, longitud, latitud "
						+ "FROM PROFECO");			
		
		// Print Schema to see column names, types and other metadata
		profecoData.printSchema();
//		
//		// Create view to execute query to get filtered data
//		roomOccupancyData.createOrReplaceTempView("ROOM_OCCUPANCY");
//		sparkSession.sql("SELECT * FROM ROOM_OCCUPANCY WHERE Temperature >= 23.6 AND Humidity > 27 AND Light > 500 "
//				+ "AND CO2 BETWEEN 920 and 950").show();
		
		// Create view to execute query to get filtered data
		profecoData.createOrReplaceTempView("PROFECOP");
//		1.a
//		sparkSession.sql("SELECT COUNT(*) FROM PROFECOP").show();		
//		1.b
//		sparkSession.sql("SELECT COUNT(DISTINCT categoria) FROM PROFECOP").show();		
//		1.c
//		sparkSession.sql("SELECT COUNT(DISTINCT cadenaComercial) FROM PROFECOP").show();	
		
//		1.e
//		sparkSession.sql(
//				 "WITH tmp AS"
//				+ "("
//				+ " SELECT MAX(CountP) as MaxCountP, estado "
//				+ " FROM ("
//				+ "	 SELECT producto, COUNT(producto) as CountP, estado "
//				+ "	 FROM PROFECOP GROUP BY producto, estado"
//				+ " )"
//				+ " GROUP BY estado"
//				+ ")"
//				+ "SELECT tmp2.producto, MaxCountP, tmp.estado "
//				+ "FROM "
//				+ "("
//				+ "	SELECT producto, COUNT(producto) as CountP, estado " 
//				+ "	FROM PROFECOP GROUP BY producto, estado"
//				+ ") as tmp2 INNER JOIN tmp ON tmp.MaxCountP = tmp2.CountP AND tmp.estado = tmp2.estado").show(200, false);	
////		1.f
//		sparkSession.sql(""
//				+ "	SELECT COUNT(DISTINCT producto, presentacion, marca) AS DisctincP, cadenaComercial  "
//				+ " FROM PROFECOP"
//				+ " GROUP BY cadenaComercial"
//				+ " ORDER BY COUNT(DISTINCT producto) DESC"
//				+ " LIMIT 1"
//				).show(200, false);
		
////		2.b
//		sparkSession.sql(""
//				+ "	SELECT municipio, SUM(precio) as sumPrecio"
//				+ " FROM PROFECOP"
//				+ " GROUP BY municipio"
//				+ " ORDER BY SUM(precio) DESC"
//				+ " LIMIT 1"
//				).show(5000, false);		
//		sparkSession.sql(""
//				+ "	SELECT municipio, SUM(precio) as sumPrecio"
//				+ " FROM PROFECOP"
//				+ " GROUP BY municipio"
//				+ " ORDER BY SUM(precio) ASC"
//				+ " LIMIT 1"
//				).show(5000, false);
		
//			2.c
		sparkSession.sql(""
		+ "	SELECT MONTH(fechaRegistro) as mes, YEAR(fechaRegistro) as annio, SUM(precio) AS sumPrecio"
		+ " FROM PROFECOP"
		+ " WHERE categoria = 'PRODUCTOS DE TEMPORADA (NAVIDEÐOS)'"
		+ " GROUP BY MONTH(fechaRegistro), YEAR(fechaRegistro)"
		+ " ORDER BY YEAR(fechaRegistro), MONTH(fechaRegistro)"
		).show(5000, false);			
		
//		2.d
//		sparkSession.sql(""
//				+ "	SELECT estado, MONTH(fechaRegistro) as mes, SUM(precio) as sumPrecio"
//				+ " FROM PROFECOP"
//				+ " GROUP BY estado, MONTH(fechaRegistro)"
//				+ " ORDER BY SUM(precio) DESC"
//				+ " LIMIT 1"
//				).show(5000, false);	
		

	}
}
