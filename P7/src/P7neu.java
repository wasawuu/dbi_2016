//dbi26
//dbdfgsi26

//Übersicht über Umsatzzahlen der Vertriebsagenten (absteigend nach Umsatzhöhe)

import java.sql.*;
import java.io.*;

public class P7neu
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
				//Verbindung mit dem Server aufbauen
				System.out.println("trying to connect\n");
				String dbUrl = "jdbc:mysql://192.168.122.63:3306/";
				Connection conn = getConnection(dbUrl, args[0], args[1]);
				System.out.println("\nConnected Server!\n");
				
				//Datenbank droppen, sofern sie existiert. Daraufhin erstellung der Datenbank
				Statement st = conn.createStatement();
				st.execute("drop database if exists Benchdb ");
				st.execute("create database Benchdb ");
				System.out.println("\ndb created\n");
				
				conn.close();//Trennung von der Datenbank
				
				//erneute Verbindung mit der DB, um rewriteBatchedStatements zu aktivieren
				conn = getConnection(dbUrl+"Benchdb?rewriteBatchedStatements=true",args[0],args[1]);
				System.out.println("\nconnected to Benchdb\n");
				st = conn.createStatement();
				
				//autocommit ausschalten, um nicht jede kleinigkeit zu commiten
				conn.setAutoCommit(false);
				
				//alle Tabellen erstellen
				st.execute("create table branches ( "+
						"branchid int not null, "+
						"branchname char(20) not null, "+
						"balance int not null, "+
						"address char(72) not null, "+
						"primary key (branchid));");
						
				System.out.println("\nbranch\n");
				
				st.execute("create table accounts ( "+
						"accid int not null, "+
						"name char(20) not null, "+
						"balance int not null, "+
						"branchid int not null, "+
						"address char(68) not null, "+
						"primary key (accid), "+
						"foreign key (branchid) references branches(branchid));");
				
				System.out.println("\naccount\n");
				
				st.execute("create table tellers ( "+
						"tellerid int not null, "+
						"tellername char(20) not null, "+
						"balance int not null, "+
						"branchid int not null, "+
						"adress char(68) not null, "+
						"primary key (tellerid), "+
						"foreign key (branchid) references branches(branchid));");

				System.out.println("\ntellers\n");
				
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
				
				conn.commit();		
				
				//Zeit vor dem Hinzufügen merken
				long t1 = System.currentTimeMillis();
				
				//n aus den beim Start übergebenen Parametern ermitteln
				int n = Integer.parseInt(args[2]);
				
				//PreparedStatement für branches erstellen
				PreparedStatement stmt = conn.prepareStatement(
					"insert into branches "+
					"values (?,?,?,?)");
				
				System.out.println("insert into branches");
				
				//n Branches-Einträge erstellen
				for(int i = 0; i < n; i++)
				{
					stmt.setInt(1, i);
					stmt.setString(2,"Sparkasse!!11einself");
					stmt.setInt(3, 0);
					stmt.setString(4,"KXHWCFTYADIJWONYIOENPUZJXTLTPPHBOOXRTTRVYBGVQWVYMCMBYNKVHXRWMYZAOXTJSLGQ");
					stmt.addBatch();
				}
				stmt.executeBatch();
				
				stmt.close();
				
				//PreparedStatement für accounts erstellen
				stmt = conn.prepareStatement(
						"insert into accounts "+
						"values (?,?,?,?,?)");

				System.out.println("insert into accounts");
				
				//n*100000 Account-Einträge einfügen
				for(int i = 0; i < n*100000; i++)
				{
					stmt.setInt(1, i);
					stmt.setString(2,"AccountsD!!11einself");
					stmt.setInt(3, 0);
					stmt.setInt(4,(int) (Math.random()*n));
					stmt.setString(5,"AFVVJFQZDWKBTDPJOETDVJKNBNJEMSGOXDKJICIMZTPSYGBNLUQGGAVHQENCWTRUXDFH");
					stmt.addBatch();
					//alle 50000 Einträge die Batch ausführen, um einen OutOfMemory Error zu vermeiden. Im Anschluss commiten.
					if(i % 50000 == 0)
					{
						stmt.executeBatch();
						conn.commit();
					}
				}
				stmt.executeBatch();	//Resteinträge ausführen
				
				stmt.close();
				
				//PreparedStatement für tellers erstellen
				stmt = conn.prepareStatement(
						"insert into tellers "+
						"values (?,?,?,?,?)");

				System.out.println("insert into tellers");
				
				//n*10 Tellers-Einträge einfügen
				for(int i = 0; i < n*10; i++)
				{
					stmt.setInt(1, i);
					stmt.setString(2,"Tellersss!!11einself");
					stmt.setInt(3, 0);
					stmt.setInt(4,(int) (Math.random()*n));
					stmt.setString(5,"AFVVJFQZDWKBTDPJOETDVJKNBNJEMSGOXDKJICIMZTPSYGBNLUQGGAVHQENCWTRUXDFH");
					stmt.addBatch();
				}
				stmt.executeBatch();		
				
				//die restlichen Accounts, sofern noch welche nicht commited sind, und alle tellers commiten
				conn.commit();
				
				stmt.close();
				
				//Endzeit abfragen und insgesamt benötigte Zeit berechnen
				t1 = System.currentTimeMillis() - t1;
				conn.close();
				System.out.println("Disconnected");
				System.out.println("Dauer:"+ t1);
			
			}
			catch(SQLException e)
			{
				System.err.println(e);
				System.exit(1);
			}
		}
	}
}