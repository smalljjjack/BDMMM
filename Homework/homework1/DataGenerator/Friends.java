import java.util.Random;

public class Friends {
	int FriendRel;
	int PersonID;
	int MyFriend;
	int DataofFriendShip;
	String Desc;
	
	private static final String[] Descs = {"Unknown","College Friend","Couple", "enemy","He used to love her", "She used to be his GF", "She owns him $2200",
			"What's a giant", "Breaking bad", "zombie", "The walk of Dead"};
	
	public Friends(int friendRel, int personID, int myFriend, int dataofFriendShip) {
		FriendRel = friendRel;
		PersonID = personID;
		MyFriend = myFriend;
		DataofFriendShip = dataofFriendShip;
		Random R = new Random();
		Desc = Descs[R.nextInt(Descs.length)];
	}

	

	public int getFriendRel() {
		return FriendRel;
	}

	public void setFriendRel(int friendRel) {
		FriendRel = friendRel;
	}

	public int getPersonID() {
		return PersonID;
	}

	public void setPerson_ID(int personID) {
		PersonID = personID;
	}

	public int getMyFriend() {
		return MyFriend;
	}

	public void setMyFriend(int myFriend) {
		MyFriend = myFriend;
	}

	public int getDataofFriendShip() {
		return DataofFriendShip;
	}

	public void setDataofFriendShip(int dataofFriendShip) {
		DataofFriendShip = dataofFriendShip;
	}

	public String getDesc() {
		return Desc;
	}

	public void setDesc(String desc) {
		Desc = desc;
	}
	
	
}
