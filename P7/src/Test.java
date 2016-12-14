import java.sql.*;

//arr yaharr

public class Test extends Thread{

	Connection conn;
	PreparedStatement st1, st2a, st2b, st2c, st2d, st2e, st3;
	
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
	
	public int meth1(int accid) throws Exception
	{
		st1.setInt(1, accid);
		ResultSet rs = st1.executeQuery();
		rs.next();
		return rs.getInt("balance");
	}
	
	public int meth2(int accid, int tellerid, int branchid, int delta) throws Exception
	{
		st2a.setInt(1, delta);
		st2a.setInt(2, branchid);
		st2a.execute();
		
		st2b.setInt(1, delta);
		st2b.setInt(2, tellerid);
		st2b.execute();
		
		st2c.setInt(1, delta);
		st2c.setInt(2, accid);
		st2c.execute();
		
		st2d.setInt(1, accid);
		ResultSet rs = st2d.executeQuery();
		rs.next();
		
		st2e.setInt(1, accid);
		st2e.setInt(2, tellerid);
		st2e.setInt(3, delta);
		st2e.setInt(4, branchid);
		st2e.setInt(5, rs.getInt("balance"));
		st2e.setString(6, "Ueberweisung auf dieses Konto.");
		st2e.execute();
		return rs.getInt("balance");
	}
	
	public int meth3(int delta) throws Exception
	{
		st3.setInt(1, delta);
		ResultSet rs = st3.executeQuery();
		rs.next();
		return rs.getInt("count(delta)");
	}

	public Test(String[] args)
	{
		try 
		{
			System.out.println("Trying to connect\n");
			conn = getConnection("jdbc:mysql://192.168.122.63:3306/Benchdb",args[0],args[1]);
			System.out.println("\nconnected to Benchdb\n");
			
			
			st1 = conn.prepareStatement("select distinct balance from accounts where accid = ?;");
			st2a = conn.prepareStatement("update branches set balance = balance + ? where branchid = ?; ");
			st2b = conn.prepareStatement("update tellers set balance = balance + ? where tellerid = ?; ");
			st2c = conn.prepareStatement("update accounts set balance = balance + ? where accid = ?; ");
			st2d = conn.prepareStatement("select distinct balance from accounts where accid = ?; ");
			st2e = conn.prepareStatement("insert into history values (?,?,?,?,?,?); ");
			st3 = conn.prepareStatement("select count(delta) from history where delta = ?;");

			conn.setAutoCommit(false);
		} 
		catch (SQLException e)
		{
			System.out.println("Fehler\n");
		}
	}
	

	public void runTest()
	{

		int count = 0;
		try
		{

			
			
			////////////////////Test 10 Eintraege
			/*
			int f = 0;
			for(f = 0;f < 10;f++)
			{
				int i = (int) (Math.random()*100);
				System.out.println(i+"\n");
				if(i < 35)
				{
					System.out.println("1 "+meth1((int) (Math.random()*10000000)));
				}
				else if(i < 85)
				{
					System.out.println("2 "+meth2((int) (Math.random()*10000000),(int) (Math.random()*1000),(int) (Math.random()*100),(int) (Math.random()*10000)));
				}
				else
				{
					System.out.println("3 "+meth3((int) (Math.random()*10000)));
				}
				conn.commit();
				//Thread.sleep(50);
			}
			*/
			
			
			long startzeit = System.currentTimeMillis();
			
			while( (System.currentTimeMillis()-startzeit)<240000 ) 	//4 Minuten einlaufzeit
			{
				int i = (int) (Math.random()*100);
				if(i < 35)
				{
					meth1((int) (Math.random()*10000000));
				}
				else if(i < 85)
				{
					meth2((int) (Math.random()*10000000),(int) (Math.random()*1000),(int) (Math.random()*100),(int) (Math.random()*10000));
				}
				else
				{
					meth3((int) (Math.random()*10000));
				}
				conn.commit();
				Thread.sleep(50);
			}
			
			startzeit = System.currentTimeMillis();
			while( (System.currentTimeMillis()-startzeit)<300000 ) 	//5 minuten Messzeit
			{
				count++;
				int i = (int) (Math.random()*100);
				if(i < 35)
				{
					meth1((int) (Math.random()*10000000));
				}
				else if(i < 85)
				{
					meth2((int) (Math.random()*10000000),(int) (Math.random()*1000),(int) (Math.random()*100),(int) (Math.random()*10000));
				}
				else
				{
					meth3((int) (Math.random()*10000));
				}
				conn.commit();
				Thread.sleep(50);
			}
			startzeit = System.currentTimeMillis();
			while( (System.currentTimeMillis()-startzeit)<60000 ) 	//1 Minute Abklingzeit
			{
				
				int i = (int) (Math.random()*100);
				//System.out.println(i);
				if(i < 35)
				{
					meth1((int) (Math.random()*10000000));
				}
				else if(i < 85)
				{
					meth2((int) (Math.random()*10000000),(int) (Math.random()*1000),(int) (Math.random()*100),(int) (Math.random()*10000));
				}
				else
				{
					meth3((int) (Math.random()*10000));
				}
				conn.commit();
				Thread.sleep(50);
			}
		}
		catch(Exception e)
		{
			System.out.println("Arr yaharr");
			e.printStackTrace();
		}
		
		System.out.println("Thread "+this.getId()+": "+count);
		
	}
	
	
	public void clearHistory(){
		Statement st;
		try {
			st = conn.createStatement();
			st.execute("Delete from history where 1=1");
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void run(){
		this.runTest();
	}
	
	public static void main(String[] args) 
	{
		Test T1 = new Test(args);
		Test T2 = new Test(args);
		Test T3 = new Test(args);
		Test T4 = new Test(args);
		Test T5 = new Test(args);
		T1.clearHistory();
		
		T1.start();
		T2.start();
		T3.start();
		T4.start();
		T5.start();
		
		try {
			T1.join();
			T2.join();
			T3.join();
			T4.join();
			T5.join();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
	}
}
