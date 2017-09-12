import java.util.Random;

public class AccessLog {
	int AccessID;
	int ByWho;
	int WhatPage;
	String TypeOfAccess;
	int AccessTime;
	static final String[] TypeOfAccesses = {"view", "like","left a note","add a friendship", "defriend", "invite a event"};
	
	public AccessLog(int accessID, int byWho, int whatPage, int accessTime) {
		
		AccessID = accessID;
		ByWho = byWho;
		WhatPage = whatPage;
		Random R = new Random();
		TypeOfAccess = TypeOfAccesses[R.nextInt(TypeOfAccesses.length)];
		AccessTime = accessTime;
	}

	public int getAccessID() {
		return AccessID;
	}

	public void setAccessID(int accessID) {
		AccessID = accessID;
	}

	public int getByWho() {
		return ByWho;
	}

	public void setByWho(int byWho) {
		ByWho = byWho;
	}

	public int getWhatPage() {
		return WhatPage;
	}

	public void setWhatPage(int whatPage) {
		WhatPage = whatPage;
	}

	public String getTypeOfAccess() {
		return TypeOfAccess;
	}

	public void setTypeOfAccess(String typeOfAccess) {
		TypeOfAccess = typeOfAccess;
	}

	public int getAccessTime() {
		return AccessTime;
	}

	public void setAccessTime(int accessTime) {
		AccessTime = accessTime;
	}
	
	
}
