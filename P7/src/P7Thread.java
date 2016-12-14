import java.sql.*;

public class P7Thread extends Thread {
	private int start, end, n;
	Connection conn;

	P7Thread(int start, int end, int n,String dbUrl, String user, String pw) throws SQLException {
		this.start = start;
		this.end = end;
		this.n = n;
		conn = getConnection(dbUrl, user, pw);;
	}
	
	protected static Connection getConnection(String url, String id, String psw) throws SQLException
	{
		try
		{
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection(url, id, psw);
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException("JDBC driver not found!");
		}
	}

	public void run() {
		System.out.println("Starte Thread "+ this.getId() +"...");
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement("insert into accounts " + "values (?,?,?,?,?)");

			System.out.println("Thread "+ this.getId() +" gestartet!");
			
			for (int i = start; i < end; i++) {
				stmt.setInt(1, i);
				stmt.setString(2, "AccountsD!!11einself");
				stmt.setInt(3, 0);
				stmt.setInt(4, (i % n));
				stmt.setString(5, "AFVVJFQZDWKBTDPJOETDVJKNBNJEMSGOXDKJICIMZTPSYGBNLUQGGAVHQENCWTRUXDFH");
				stmt.addBatch();
				if (i%100000 == 0) {
					System.out.println("Thread "+ this.getId() +": " + (i-start) + " von " + (end-start) + " Accounts einfügen");
					stmt.executeBatch();
					//conn.commit(); // bessere performance je nach Bufferplatz
									// des Servers
					System.out.println("Tread"+ this.getId() +": eingefügt!\n");
				}

			}

			System.out.println("Thread beendet!");
			stmt.executeBatch();
			conn.close();
			} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
