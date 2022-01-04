/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package queryrunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

/**
 *
 * QueryRunner takes a list of Queries that are initialized in it's constructor
 * and provides functions that will call the various functions in the QueryJDBC class 
 * which will enable MYSQL queries to be executed. It also has functions to provide the
 * returned data from the Queries. Currently the eventHandlers in QueryFrame call these
 * functions in order to run the Queries.
 */
public class QueryRunner {

    public QueryRunner()
    {
        this.m_jdbcData = new QueryJDBC();
        m_updateAmount = 0;
        m_queryArray = new ArrayList<>();
        m_error="";

        this.m_projectTeamApplication="BASEBALL";    // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

        // Query 1: Show all players
        m_queryArray.add(new QueryData("Select * from PLAYER", null, null, false, false));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

        // Query 2: Shows positions of players
        m_queryArray.add(new QueryData("SELECT DISTINCT POSITIONDESC AS 'POSITION', FIRSTNAME, LASTNAME, FIRSTSTART AS 'FIRST START', COUNTRY, LANGUAGE, TYPE AS 'HOBBY'\n" +
                "FROM PLAYER\n" +
                "LEFT JOIN POSITION ON PLAYER.POSITON=POSITION.POSITIONID\n" +
                "LEFT JOIN COUNTRYORGIN ON PLAYER.IDPLAYER=COUNTRYORGIN.IDPLAYER\n" +
                "LEFT JOIN HOBBIES ON PLAYER.IDPLAYER=HOBBIES.IDPLAYER\n" +
                "LEFT JOIN LANGUAGE ON PLAYER.IDPLAYER=LANGUAGE.IDPLAYER\n" +
                "WHERE POSITON = ?", new String [] {"POSITION NUMBER"}, new boolean [] {true}, false, true));        // THIS NEEDS TO CHANGE FOR YOUR APPLICATION


        // Query 3: Top 20 players for SLUGGING in 2019
        m_queryArray.add(new QueryData("SELECT FIRSTNAME, LASTNAME, SUM(SINGLE+(2*DOUBLES)+(3*TRIPLE)+(4*HR)) AS 'Total', YEARNUM AS 'YEAR'\n" +
                "FROM PLAYER P JOIN BATTING B ON P.idplayer = B.idPlayer\n" +
                "WHERE YEARNUM=?\n" +
                "GROUP BY FIRSTNAME,LASTNAME, YEARNUM\n" +
                "ORDER BY SUM(SINGLE+(2*DOUBLES)+(3*TRIPLE)+(4*HR)) DESC\n" +
                "LIMIT 20", new String [] {"YEAR"}, new boolean [] {true}, false, true));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

        // Query 4: First Start
        m_queryArray.add(new QueryData("SELECT FIRSTNAME, LASTNAME, FIRSTSTART, SUM(STRIKES) as 'TOTAL STRIKES',\n" +
                "SUM(BALLS) as 'TOTAL BALLS'\n" +
                "FROM PLAYER P JOIN GAMEDATA GD ON P.IDPLAYER = GD.IDPLAYER\n" +
                "WHERE FIRSTSTART > ?\n" +
                "GROUP by FIRSTNAME,LASTNAME,FIRSTSTART\n" +
                "ORDER BY LASTNAME ASC", new String [] {"YEAR"}, new boolean [] {true}, false, true));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION
//                "WHERE FIRSTSTART > '2018-01-01'\n" +

        // Query 5: GameID
        m_queryArray.add(new QueryData("SELECT COUNT(BALLS) AS 'Number of Balls: April 9 2019'\n" +
                "FROM GAMEDATA\n" +
                "WHERE GAMEID = 'BOS201904090'", null, null, false, false));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

        // Query 6: Team Name
        m_queryArray.add(new QueryData("SELECT FIRSTNAME, LASTNAME, TEAM, GAMES\n" +
                "FROM PLAYER P\n" +
                "LEFT JOIN PITCH ON P.IDPLAYER=PITCH.IDPLAYER\n" +
                "LEFT JOIN ROSTER R ON P.IDPLAYER=R.IDPLAYER\n" +
                "WHERE TEAM LIKE ? AND GAMES>?\n" +
                "ORDER BY GAMES DESC;",  new String [] {"TEAM NAME", "GAMES PLAYED"}, new boolean [] {true, true}, false, true));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION


        // Query 7
        m_queryArray.add(new QueryData("SELECT DISTINCT R.YEARNUM AS 'YEAR', TEAM AS 'TEAM', FIRSTNAME AS 'FIRST', LASTNAME AS 'LAST', \n" +
                "\t\tGAMES, AB, HIT, (HIT/AB) AS BATTING_AVERAGE\n" +
                "FROM PLAYER P\n" +
                "INNER JOIN BATTING B ON P.IDPLAYER=B.IDPLAYER\n" +
                "INNER JOIN ROSTER R ON P.IDPLAYER=R.IDPLAYER\n" +
                "WHERE R.TEAM LIKE ? AND R.YEARNUM=?\n" +
                "ORDER BY BATTING_AVERAGE DESC", new String [] {"TEAM NAME", "YEAR"}, new boolean [] {true, true}, false, true));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

        // Query 8
        m_queryArray.add(new QueryData("SELECT FIRSTNAME, LASTNAME, HEIGHT, WEIGHT, (HIT/AB) AS BATTING_AVERAGE\n" +
                "FROM PLAYER P\n" +
                "INNER JOIN BATTING B ON P.IDPLAYER=B.IDPLAYER\n" +
                "INNER JOIN ROSTER R ON P.IDPLAYER=R.IDPLAYER\n" +
                "WHERE (HIT/AB)> (\n" +
                "\tSELECT AVG(HIT/AB) AS BATTING_AVERAGE\n" +
                "\tFROM PLAYER P\n" +
                "\tLEFT JOIN BATTING B ON P.IDPLAYER=B.IDPLAYER\n" +
                "\tINNER JOIN ROSTER R ON P.IDPLAYER=R.IDPLAYER\n" +
                "    WHERE R.YEARNUM=? AND HIT !=0\n" +
                ")\n" +
                "ORDER BY BATTING_AVERAGE DESC", new String [] {"YEAR"}, new boolean [] {true}, false, true));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION


        // Query 9
        m_queryArray.add(new QueryData("SELECT DISTINCT FIRSTNAME, LASTNAME, POSITIONDESC, (HIT/AB) AS BAT_AVG\n" +
                "FROM PLAYER P\n" +
                "LEFT JOIN BATTING B ON P.IDPLAYER=B.IDPLAYER\n" +
                "INNER JOIN(\n" +
                "\tSELECT MAX(HIT/AB) AS BATTING_AVERAGE, Positon\n" +
                "\tFROM PLAYER P\n" +
                "\tLEFT JOIN BATTING B ON P.IDPLAYER=B.IDPLAYER\n" +
                "\tGROUP BY POSITON\n" +
                ") S ON P.POSITON = S.POSITON AND ROUND((HIT/AB),4)=ROUND(S.BATTING_AVERAGE,4)\n" +
                "LEFT JOIN POSITION ON P.POSITON=POSITION.POSITIONID\n" +
                "WHERE POSITION.POSITIONDESC IS NOT NULL AND POSITIONDESC != \"Pitcher\";", null, null, false, false));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION


        // Query 10
        m_queryArray.add(new QueryData("SELECT GAMEID, FIRSTNAME, LASTNAME, SUM(BALLS) AS \"BALLS\", SUM(STRIKES) AS \"STRIKES\", MAX(HOMESCORE) AS \"HOMESCORE\", MAX(OPPSCORE) AS \"OPPSCORE\"\n" +
                "FROM GAMEDATA G\n" +
                "INNER JOIN PLAYER P ON G.IDPLAYER = P.IDPLAYER\n" +
                "WHERE P.IDPLAYER=(\n" +
                "\tSELECT P.IDPLAYER\n" +
                "\tFROM PLAYER P\n" +
                "\tLEFT JOIN BATTING B ON P.IDPLAYER=B.IDPLAYER\n" +
                "\tRIGHT JOIN ROSTER R ON P.IDPLAYER=R.IDPLAYER\n" +
                "\tWHERE R.TEAM LIKE ? AND R.YEARNUM=2019\n" +
                "\tORDER BY (HIT/AB) DESC LIMIT 1\n" +
                ") AND OPPID LIKE ? \n" +
                "GROUP BY GAMEID, FIRSTNAME, LASTNAME;\n",new String [] {"TEAM NAME", "OPPOSING TEAM"}, new boolean [] {true, true}, false, true));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

        // Query 11
        m_queryArray.add(new QueryData("SELECT FIRSTNAME, LASTNAME, SINGLE, DOUBLES, TRIPLE, HR, (HIT/AB) AS BATTING_AVERAGE\n" +
                "FROM PLAYER P\n" +
                "LEFT JOIN  BATTING B ON P.IDPLAYER=B.IDPLAYER\n" +
                "ORDER BY (HIT/AB) DESC \n" +
                "LIMIT 10\n", null, null, false, false));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

    }

    public int GetTotalQueries()
    {
        return m_queryArray.size();
    }

    public int GetParameterAmtForQuery(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetParmAmount();
    }

    public String  GetParamText(int queryChoice, int parmnum)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetParamText(parmnum);
    }

    public String GetQueryText(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetQueryString();
    }

    /**
     * Function will return how many rows were updated as a result
     * of the update query
     * @return Returns how many rows were updated
     */

    public int GetUpdateAmount()
    {
        return m_updateAmount;
    }

    /**
     * Function will return ALL of the Column Headers from the query
     * @return Returns array of column headers
     */
    public String [] GetQueryHeaders()
    {
        return m_jdbcData.GetHeaders();
    }

    /**
     * After the query has been run, all of the data has been captured into
     * a multi-dimensional string array which contains all the row's. For each
     * row it also has all the column data. It is in string format
     * @return multi-dimensional array of String data based on the resultset
     * from the query
     */
    public String[][] GetQueryData()
    {
        return m_jdbcData.GetData();
    }

    public String GetProjectTeamApplication()
    {
        return m_projectTeamApplication;
    }
    public boolean  isActionQuery (int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.IsQueryAction();
    }

    public boolean isParameterQuery(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.IsQueryParm();
    }


    public boolean ExecuteQuery(int queryChoice, String [] parms)
    {
        boolean bOK = true;
        QueryData e=m_queryArray.get(queryChoice);
        bOK = m_jdbcData.ExecuteQuery(e.GetQueryString(), parms, e.GetAllLikeParams());
        return bOK;
    }

    public boolean ExecuteUpdate(int queryChoice, String [] parms)
    {
        boolean bOK = true;
        QueryData e=m_queryArray.get(queryChoice);
        bOK = m_jdbcData.ExecuteUpdate(e.GetQueryString(), parms);
        m_updateAmount = m_jdbcData.GetUpdateCount();
        return bOK;
    }

    public boolean Connect(String szHost, String szUser, String szPass, String szDatabase)
    {

        boolean bConnect = m_jdbcData.ConnectToDatabase(szHost, szUser, szPass, szDatabase);
        if (bConnect == false)
            m_error = m_jdbcData.GetError();
        return bConnect;
    }

    public boolean Disconnect()
    {
        // Disconnect the JDBCData Object
        boolean bConnect = m_jdbcData.CloseDatabase();
        if (bConnect == false)
            m_error = m_jdbcData.GetError();
        return true;
    }

    public String GetError()
    {
        return m_error;
    }

    private QueryJDBC m_jdbcData;
    private String m_error;
    private String m_projectTeamApplication;
    private ArrayList<QueryData> m_queryArray;
    private int m_updateAmount;

    /**
     * @param args the command line arguments
     */


    public static void main(String[] args) {
        final QueryRunner queryrunner = new QueryRunner();

        if (args.length == 0)
        {
            java.awt.EventQueue.invokeLater(new Runnable() {
                public void run() {

                    new QueryFrame(queryrunner).setVisible(true);
                }
            });
        }
        else
        {
            if (args[0].equals ("-console"))
            {
                String host,user,pass,database;
                QueryJDBC connection = new QueryJDBC();
                Scanner keyboard = new Scanner(System.in);
                System.out.println("Host: ");
                host = keyboard.nextLine();
                System.out.println("User: ");
                user = keyboard.nextLine();
                System.out.println("Password: ");
                pass = keyboard.nextLine();
                System.out.println("Database: ");
                database=keyboard.nextLine();
                System.out.println("Welcome to the Baseball for Beginners app, where our mission is to help non-Major League Baseball watchers enter the baseball world!");


                connection.ConnectToDatabase(host, user, pass, database);
                System.out.println("Connected!");

                int n = queryrunner.GetTotalQueries();
                System.out.println("Total Queries: "+n);
                      for (int i=0;i < n; i++)
                     {
                         System.out.println("i is: "+i);
                         int amt = queryrunner.GetParameterAmtForQuery(i);
                         String [] parameterArray=new String[amt];
                         String [] headers;
                         String [][] allData;
                         if(queryrunner.isParameterQuery(i)){
                             for(int j = 0; j<amt; j++){
                                 System.out.println(queryrunner.GetParamText(i, j) + " Enter value: ");
                                 parameterArray[j]=keyboard.nextLine();
                                 System.out.println("Parameter Array: "+parameterArray[j]);
                             }

                         if(queryrunner.isActionQuery(i)){
                             queryrunner.ExecuteUpdate(i, parameterArray);
                             System.out.println("Rows Affected: " +queryrunner.GetUpdateAmount());
                         }
                         else{
                             queryrunner.ExecuteQuery(i, parameterArray);
                             headers = queryrunner.GetQueryHeaders();
                             allData = queryrunner.GetQueryData();
                             System.out.println(Arrays.toString(headers));
                             System.out.println(Arrays.deepToString(allData));
                         }
                     }
                 }
                      queryrunner.Disconnect();
            }
        }
    }
}