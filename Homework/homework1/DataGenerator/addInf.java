import java.util.*;
import java.io.*;

public class addInf {
	
	public static void main(String[] args) throws Exception {
        
		RandomGenerator randomGenerator = new RandomGenerator();
		Random R = new Random();
        
		//generate MyPages
		String MypageFile = "D:/WPIStudyFile/CS585/Project1/Data/Mypage.csv";
        FileWriter MypageWriter = new FileWriter(MypageFile);

        int Page_COUNT = 1000000;
        List<Mypage> pages = new ArrayList<Mypage>(Page_COUNT);

        for (int i=1; i<=Page_COUNT; i++) {
            System.out.println(String.format("ID: %d", i));

            String name = randomGenerator.getRandomString(10, 20);
            String nationality = randomGenerator.getRandomString(10,20);
            int countryCode = randomGenerator.getRandomIntegerBetween(1, 10);
            String hobby = randomGenerator.getRandomString(10, 20);

            Mypage page = new Mypage(i, name, nationality, countryCode, hobby);
            pages.add(page);
        }

        System.out.println("Writing Mypage to file...");
        
        for (Mypage page : pages) {
            List<String> list = new ArrayList<String>();
            list.add(Integer.toString(page.getID()));
            list.add(page.getName());
            list.add(page.getNationality());
            list.add(Integer.toString(page.getCountryCode()));
            list.add(page.getHobby());

            CsvUtils.writeLine(MypageWriter, list);
        }

        MypageWriter.flush();
        MypageWriter.close();
        System.out.println("Writing Friends to file...");
       //generate Friends
      		String FriendsFile = "D:/WPIStudyFile/CS585/Project1/Data/Friends.csv";
              FileWriter FriendsWriter = new FileWriter(FriendsFile);

              int Friends_COUNT = 10000000;
              List<Friends> friends = new ArrayList<Friends>(Friends_COUNT);

              for (int i=1; i<=Friends_COUNT; i++) {
                  System.out.println(String.format("ID: %d", i));

                  int personID = R.nextInt(100000-1)+1;
                  int myFriend = R.nextInt(100000-1)+1;
                  int DateofFriendship = randomGenerator.getRandomIntegerBetween(1, 1000000);

                  Friends friend = new Friends(i,personID, myFriend, DateofFriendship);
                  friends.add(friend);
              }
              
              for (Friends friend : friends) {
                  List<String> list = new ArrayList<String>();
                  list.add(Integer.toString(friend.getFriendRel()));
                  list.add(Integer.toString(friend.getPersonID()));
                  list.add(Integer.toString(friend.getMyFriend()));
                  list.add(Integer.toString(friend.getDataofFriendShip()));
                  list.add(friend.getDesc());

                  CsvUtils.writeLine(FriendsWriter, list);
              }

              FriendsWriter.flush();
              FriendsWriter.close();
              System.out.println("Writing Friendship to file...");
        //Generate AccessLog
        String AccessLogFile = "D:/WPIStudyFile/CS585/Project1/Data/AccessLog.csv";
        FileWriter AccessLogWriter = new FileWriter(AccessLogFile);

        int AccessLog_COUNT = 10000000;
        List<AccessLog> AccessLogs = new ArrayList<AccessLog>(AccessLog_COUNT);

        for (int i=1; i<=AccessLog_COUNT; i++) {
        	System.out.println(String.format("ID: %d", i));

            int byWho = R.nextInt(100000-1)+1;
            int whatPage = R.nextInt(100000-1)+1;
            int accessTime = randomGenerator.getRandomIntegerBetween(1, 1000000);
            AccessLog accesslog = new AccessLog(i,byWho, whatPage, accessTime);
            AccessLogs.add(accesslog);
        }
        
        for (AccessLog accessLog : AccessLogs) {
            List<String> list = new ArrayList<String>();
            list.add(Integer.toString(accessLog.getAccessID()));
            list.add(Integer.toString(accessLog.getByWho()));
            list.add(Integer.toString(accessLog.getWhatPage()));
            list.add(accessLog.getTypeOfAccess());
            list.add(Integer.toString(accessLog.getAccessTime()));

            CsvUtils.writeLine(AccessLogWriter, list);
        }

        AccessLogWriter.flush();
        AccessLogWriter.close();
        System.out.println("Writing AccessLog to file...");
	}
}
