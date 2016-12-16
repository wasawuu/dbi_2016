//dbi26
//dbdfgsi26

//Übersicht über Umsatzzahlen der Vertriebsagenten (absteigend nach Umsatzhöhe)

import java.sql.*;
import java.io.*;

public class P7
{
	protected static BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
	
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
	
	protected static String getInput(String prompt)
	{
		try
		{
			System.out.print(prompt);
			return stdin.readLine();
		}
		catch (IOException e)
		{
			System.err.println(e);
			return null;
		}
	}
	
	public static void main(String[] args)
	{
		if (args.length!=3)
		{
			System.err.println(
			"\nusage: java CustomerOverview <userid> <passwd>\n");
			System.exit(1);
		}
		else
		{
			try
			{
				String dbUrl = "jdbc:mysql://192.168.122.63:3306/";				//192.168.122.63
				Connection conn = getConnection(dbUrl, args[0], args[1]);
				System.out.println("\nConnected Server!\n");
				
				
				
				Statement st = conn.createStatement();
				st.execute("drop database if exists Benchdb ");
				st.execute("create database Benchdb ");
				System.out.println("\ndb created\n");
				//conn.setCatalog("Benchdb");
				
				conn.close();
				conn = getConnection(dbUrl + "Benchdb?rewriteBatchedStatements=true", args[0], args[1]);  //rewrite Batched Statements
				
				System.out.println("\nconnected to Benchdb\n");
				st = conn.createStatement();
			
				conn.setAutoCommit(false);
				st.execute("create table branches ( "+
						"branchid int not null, "+
						"branchname char(20) not null, "+
						"balance int not null, "+
						"address char(72) not null, "+
						"primary key (branchid));");
						
				System.out.println("\nbranches erstellt\n");
				
				st.execute("create table accounts ( "+
						"accid int not null, "+
						"name char(20) not null, "+
						"balance int not null, "+
						"branchid int not null, "+
						"address char(68) not null, "+
						"primary key (accid), "+
						"foreign key (branchid) references branches(branchid));");
				
				System.out.println("\naccounts erstellt\n");
				
				st.execute("create table tellers ( "+
						"tellerid int not null, "+
						"tellername char(20) not null, "+
						"balance int not null, "+
						"branchid int not null, "+
						"adress char(68) not null, "+
						"primary key (tellerid), "+
						"foreign key (branchid) references branches(branchid));");

				System.out.println("\ntellers erstellt\n");
				
				st.execute("create table history ( "+
						"accid int not null, "+
						"tellerid int not null, "+
						"delta int not null, "+
						"branchid int not null, "+
						"accbalance int not null, "+
						"cmmnt char(30) not null, "+
						"foreign key (accid) references accounts (accid), "+
						"foreign key (tellerid) references tellers(tellerid), "+
						"foreign key (branchid) references branches(branchid));");
				
				System.out.println("\nhistory erstellt\n");
				conn.commit();
				System.out.println("Committed!");
				
				
				//Tabellen Erstellt
				
				long t1 = System.currentTimeMillis(); //Zeit nehmen
				
				
				//Branches
				PreparedStatement stmt = conn.prepareStatement(
					"insert into branches "+
					"values (?,?,?,?)");
				
				int n = Integer.parseInt(args[2]);
				
				System.out.println("insert into branches:");
				for(int i = 0; i < n; i++)
				{
					stmt.setInt(1, i);
					stmt.setString(2,"Sparkasse!!11einself");
					stmt.setInt(3, 0);
					stmt.setString(4,"KXHWCFTYADIJWONYIOENPUZJXTLTPPHBOOXRTTRVYBGVQWVYMCMBYNKVHXRWMYZAOXTJSLGQ");
					stmt.addBatch();
				}
				stmt.executeBatch();
				
				
				//Accounts
				/*
				stmt = conn.prepareStatement(
						"insert into accounts "+
						"values (?,?,?,?,?)");
				*/
				
				int total = n*100000;
				
				P7Thread p1 = new P7Thread(0, (total/4), n, dbUrl + "Benchdb?rewriteBatchedStatements=true", args[0], args[1]);
				/*
				P7Thread p2 = new P7Thread((total/4), (total/4)*2, n, dbUrl + "Benchdb?rewriteBatchedStatements=true", args[0], args[1]);
				P7Thread p3 = new P7Thread((total/4)*2, (total/4)*3, n, dbUrl + "Benchdb?rewriteBatchedStatements=true", args[0], args[1]);
				P7Thread p4 = new P7Thread((total/4)*3, total, n, dbUrl + "Benchdb?rewriteBatchedStatements=true", args[0], args[1]);
				*/
				
				
				System.out.println("\n\nStarte Threads\n");
				p1.start();
				/*
				p2.start();
				p3.start();
				p4.start();
				*/
				
				try {
					p1.join();
					/*
					p2.join();
					p3.join();
					p4.join();
					*/
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
				
				/*
				System.out.println("insert into accounts:");
				for(int i = 0; i < n*100000; i++)
				{
					stmt.setInt(1, i);
					stmt.setString(2,"AccountsD!!11einself");
					stmt.setInt(3, 0);
					stmt.setInt(4, (i%n));
					stmt.setString(5,"AFVVJFQZDWKBTDPJOETDVJKNBNJEMSGOXDKJICIMZTPSYGBNLUQGGAVHQENCWTRUXDFH");
					stmt.addBatch();
					if(i % 100000 == 0){
						stmt.executeBatch();
						conn.commit();			//bessere performance je nach Bufferplatz des Servers
						System.out.println(i + " von " + (n*100000) + " Accounts eingefügt!");
					}
				}
				stmt.executeBatch();
				*/

				
				//Tellers
				stmt = conn.prepareStatement(
						"insert into tellers "+
						"values (?,?,?,?,?)");

				System.out.println("insert into tellers:");
				for(int i = 0; i < n*10; i++)
				{
					stmt.setInt(1, i);
					stmt.setString(2,"Tellersss!!11einself");
					stmt.setInt(3, 0);
					stmt.setInt(4, (i%n));
					stmt.setString(5,"AFVVJFQZDWKBTDPJOETDVJKNBNJEMSGOXDKJICIMZTPSYGBNLUQGGAVHQENCWTRUXDFH");
					stmt.addBatch();
				}		
				stmt.executeBatch();
				
				conn.commit();
				System.out.println("Tabellen gefüllt!");
				long t2 = System.currentTimeMillis() - t1;
				conn.close();
				System.out.println("Disconnected");
				System.out.println("Dauer:"+ t2);
			
			}
			catch(SQLException e)
			{
				System.err.println(e);
				System.exit(1);
			}
		}
	}
}