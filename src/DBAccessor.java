import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

public class DBAccessor {
	private String dbname;
	private String host;
	private String port;
	private String user;
	private String passwd;
	private String schema;
	Connection conn = null;

	// TODO
	/**
	 * Initializes the class loading the database properties file and assigns
	 * values to the instance variables.
	 * 
	 * @throws RuntimeException
	 *             Properties file could not be found.
	 */
	public void init() {
		// TODO
		Properties prop = new Properties();
		InputStream propStream = this.getClass().getClassLoader().getResourceAsStream("db.properties");
		try{
			prop.load(propStream);
			this.host = prop.getProperty("host");
			this.port = prop.getProperty("port");
			this.dbname = prop.getProperty("dbname");
			this.schema = prop.getProperty("schema");
		}catch(IOException e){
			String message = "ERROR: db.properties file could not be found";
			System.err.println(message);
			throw new RuntimeException(message, e);
		}
		
	}
	
	public Connection getConnection() {

		// Implement the DB connection
		String url = null;
		try {
			// Loads the driver
			Class.forName("org.postgresql.Driver");

			// Preprara connexiÃ³ a la base de dades
			StringBuffer sbUrl = new StringBuffer();
			sbUrl.append("jdbc:postgresql:");
			if (host != null && !host.equals("")) {
				sbUrl.append("//").append(host);;
				}
				if (port != null && !port.equals("")) {
					sbUrl.append(":").append(port);
			}
			sbUrl.append("/").append(dbname);
			url = sbUrl.toString();

			// Utilitza connexiÃ³ a la base de dades
			conn = DriverManager.getConnection(url, "doctor","1234");
			conn.setAutoCommit(false);
		} catch (ClassNotFoundException e1) {
			System.err.println("ERROR: Al Carregar el driver JDBC");
			System.err.println(e1.getMessage());
		} catch (SQLException e2) {
			System.err.println("ERROR: No connectat  a la BD " + url);
			System.err.println(e2.getMessage());
		}

		// Sets the search_path
		if (conn != null) {
			Statement statement = null;
			try {
				statement = conn.createStatement();
				statement.executeUpdate("SET search_path TO " + this.schema);
				// missatge de prova: verificaciÃ³
				System.out.println("OK: connectat a l'esquema " + this.schema + " de la base de dades " + url
						+ " usuari: " + user + " password:" + passwd);
				System.out.println();
				//
			} catch (SQLException e) {
				System.err.println("ERROR: Unable to set search_path");
				System.err.println(e.getMessage());
			} finally {
				try {
					statement.close();
				} catch (SQLException e) {
					System.err.println("ERROR: Closing statement");
					System.err.println(e.getMessage());
				}
			}
		}

		return conn;
	}
		

	// TODO
	public void altaVisita() throws SQLException, IOException {
		BufferedReader buff = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.println("Introdueix id del pacient:");
		int pat_number= Integer.parseInt(buff.readLine());
		mostraPacient(pat_number);
		// TODO demana per consola l'identificador del doctor
		System.out.println("Introdueix id del doctor:");
		int doc_number= Integer.parseInt(buff.readLine());
		mostraDoctor(doc_number);
		// TODO demana per consola la data de la visita i realitza la
		System.out.println("Introdueix dia de la visita:");
		int d=Integer.parseInt(buff.readLine());
		System.out.println("Introdueix mes de la visita:");
		int m=Integer.parseInt(buff.readLine());
		System.out.println("Introdueix any de la visita:");
		int a=Integer.parseInt(buff.readLine());
		LocalDate date = null;
		date= LocalDate.of(a, m, d);
		// inserciÃ³ del registre
		Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		st.executeUpdate("Insert into visit (doc_number,pat_number,visit_date) Values('"+doc_number+"','"+pat_number+"','"+date+"')");
		// confirmar la inserciÃ³!!!
		ResultSet rs = st.executeQuery("Select * from visit where pat_number = "+pat_number+" and doc_number="+doc_number);
		if (rs== null) {
			System.out.println("No trobat");
		}else{
			while (rs.next()) {
				System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t"+rs.getString(3)+"\t"+rs.getString(4));
			}
		}
		
	}

	// TODO
	public void modificaAdrecaPacient() throws SQLException, IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		ResultSet rs = null;
		Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		// TODO introdueix l'id del pacient
		System.out.println("Introdueix id del pacient: ");
		int pat_number = Integer.parseInt(br.readLine());
		mostraPacient(pat_number);
		// TODO solicita si es vol modificar el canvi de la seva adreça
		System.out.println("Vols modificar l'adreça d'aquest pacient? [si/no]: ");
		String resposta = br.readLine();
		// en cas afirmatiu s'actualitzen els camps i la fila
		if(resposta.equals("si")){
			rs=st.executeQuery("SELECT * FROM patient WHERE pat_number = "+pat_number+"");
			while(rs.next()){
				System.out.println("Introdueix la nova adreça del pacient: ");
				String adreça = br.readLine();
				System.out.println("Introdueix la nova ciutat del pacient: ");
				String ciutat = br.readLine();
				rs.updateString("address", adreça);
				rs.updateString("city", ciutat);
				rs.updateRow();
			}
			
		}else{
			System.out.println("Modificació no realitzada");
		}
		
		// es verifica si els canvis s'han realitzat
		mostraPacient(pat_number);

	}

	// TODO
	@SuppressWarnings({ "unused" })
	public void mostraDoctorsPerEspecialitat() throws SQLException, IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		ResultSet rs = null;
		Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);

		mostraEspecialitats();
		// TODO
		// En escollir el valor id una d'aquestes especialitats
		// caldrà mostrar els doctors d'aquella especialitat
		// en concret doc_number, name, address, city, phone,
		System.out.println("Tria la id de l'especialitat: ");
		int id = Integer.parseInt(br.readLine());
		rs=st.executeQuery("SELECT doc_number,name,address,city,phone FROM doctor WHERE speciality="+id+"");
		if(rs==null){
			System.out.println("No existeixen especialitats");
			}else{
				while(rs.next()){
					System.out.println(rs.getString("doc_number")+" "+rs.getString("name")+" "+rs.getString("address")+" "+
							rs.getString("city")+" "+rs.getString("phone"));
				}
			
		}

	}

	// TODO
	public void mostraEspecialitats() throws SQLException {

		// TODO
		// realitzar la consulta corresponent per tal de mostar:
		// id i name de les especialitats mèdiques
		ResultSet rs;
		Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
		rs=st.executeQuery("SELECT id,name FROM speciality");
		while(rs.next()){
			System.out.println(rs.getInt("id")+" "+rs.getString("name"));
		}
		rs.close();
		st.close();
	}

	// TODO
	public void mostraPacient(int pat_number) throws SQLException {

		// TODO
		// mostra totes les dades del pacient a partir del seu identificador
		ResultSet rs;
		Statement st;
		st=conn.createStatement();
		rs=st.executeQuery("SELECT * FROM patient where pat_number = "+pat_number+"");
		while(rs.next()){
			System.out.println(rs.getString("pat_number")+" "+rs.getString("name")
			+" "+rs.getString("address")+" "+rs.getString("city")+" "+rs.getString("dni"));
		}
		rs.close();
		st.close();

	}
	
	// TODO
	public void mostraDoctor(int doc_number) throws SQLException, NumberFormatException, IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		ResultSet rs;
		Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
		rs=st.executeQuery("SELECT doc_number,name FROM doctor");
		if(rs==null){
			System.out.println("No existeix cap doctor");
		}else{
			while(rs.next()){
				System.out.println(rs.getString("doc_number")+" "+rs.getString("name"));
			}
		}
		
		// mostra el nom del doctor i el nom de la seva especialitat
		// a partir de l'identificador del doctor
		System.out.println("Introdueix la id del doctor per veure el nom i la especialitat: ");
		int id = Integer.parseInt(br.readLine());
		rs=st.executeQuery("SELECT d.name , s.name FROM doctor d, speciality s WHERE doc_number="+id+"AND d.speciality=s.id");
		if(rs==null){
			System.out.println("No existeixen doctors");
		}else{
			while(rs.next()){
				System.out.println(rs.getString(1)+" "+rs.getString(2));
			}
		}
		

	}

	// TODO
	public void modificaSalariDoctor() throws SQLException, NumberFormatException, IOException {
		// TODO
				// demana l'identificador del doctor
				// mostra totes les dades del doctor
				// confirma si es vol modificar el salari
				// en cas afirmatiu demana nou valor del salari i
				// actualitza el camp i la fila
				// torna a mostrar totes les dades del doctor
		BufferedReader buff = new BufferedReader(new InputStreamReader(System.in));
		Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
		ResultSet rs = null;
		rs=st.executeQuery("SELECT * FROM doctor");
		if(rs == null){
			System.out.println("Llista buida");
		}else{
			while(rs.next()){
				System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t"
						+rs.getString(3)+"\t"+rs.getString(4)+"\t"+rs.getString(5)+"\t"
						+rs.getString(6)+"\t"+rs.getString(7)+"\t"+rs.getString(8)+"\t"+rs.getString(9));
			}
		}
		
		System.out.println("Introdueix Id doctor: ");
		int doc_number = Integer.parseInt(buff.readLine());
		Statement st1 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
		ResultSet rs1 = null;
		rs1=st1.executeQuery("SELECT * FROM doctor WHERE doc_number="+doc_number+"");
		while (rs1.next()) {
			System.out.println(rs1.getString(1)+"\t"+rs1.getString(2)+"\t"
					+rs1.getString(3)+"\t"+rs1.getString(4)+"\t"+rs1.getString(5)+"\t"
					+rs1.getString(6)+"\t"+rs1.getString(7)+"\t"+rs1.getString(8)+"\t"+rs1.getString(9));
			System.out.println("Vols modificar el salari?");
			String resposta = buff.readLine();
			if (resposta.equals("Si")){
				System.out.println("Introdueix nou salari: ");
				int salari = Integer.parseInt(buff.readLine());
				rs1.updateInt("salary", salari);
				rs1.updateRow();
			}
			System.out.println(rs1.getString(1)+"\t"+rs1.getString(2)+"\t"+rs1.getString(3)+"\t"+rs1.getString(4)+"\t"+rs1.getString(5)+"\t"+rs1.getString(6)+"\t"+rs1.getString(7)+"\t"+rs1.getString(8)+"\t"+rs1.getString(9));
		}
	}

	public void sortir() throws SQLException {
		System.out.println("ADÉU!");
		conn.close();
	}

	// TODO
	public void carregaVisites() throws SQLException, NumberFormatException, IOException, ParseException {
		// TODO
				// mitjançant Prepared Statement
				// per a cada línia del fitxer visites.csv
				// realitzar la inserció corresponent
		ArrayList<String>list = new ArrayList<String>();
		ResultSet rs,rs2;
		Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
		rs=st.executeQuery("SELECT * FROM visit");
		if(rs==null){
			System.out.println("No hi han visites");
		}
		while(rs.next()){
			System.out.println(rs.getString(1)+" "+rs.getString(2)+" "+rs.getString(3)+" "+rs.getString(4));
		}
		String sql = "INSERT INTO visit VALUES (?,?,?,?)";
		PreparedStatement ps = conn.prepareStatement(sql);
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Juan\\Desktop\\visites.csv"));
		
		StringTokenizer stk = null;
		Calendar cal = new GregorianCalendar();
		
		String valors[] = new String [4];
		int i = 0;
		boolean eof = false;
		do{
			String line = br.readLine();
			if(line==null){
				eof=true;
			}else{
				stk=new StringTokenizer(line,",");
				while (stk.hasMoreElements()) {
					valors[i]=stk.nextToken();
					i=i+1;
				}
			}
						
			ps.clearParameters();
			ps.setInt(1, Integer.parseInt(valors[0]));
			ps.setInt(2, Integer.parseInt(valors[1]));
			ps.setDate(3,java.sql.Date.valueOf(valors[2]),cal);
			ps.setFloat(4, Float.parseFloat(valors[3]));
			i=0;
		}while(eof!=true);
		
		System.out.println("Dades carregades OK");
		rs=st.executeQuery("SELECT * FROM visit");
		
		if(rs==null){
			System.out.println("No hi han visites");
		}
		while(rs.next()){
			System.out.println(rs.getString(1)+" "+rs.getString(2)+" "+rs.getString(3)+" "+rs.getString(4));
			
		}
		rs2=st.executeQuery("SELECT count(*) FROM visit");
		if(rs2==null){
			System.out.println("err");
		}
		while(rs2.next()){
			System.out.println(rs2.getString(1));
		}
		
	}
}
