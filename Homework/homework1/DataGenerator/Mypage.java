import java.math.*;
import java.util.Random;
public class Mypage {
	 int ID;
	 String Name;
	 String Nationality;
	 int CountryCode;
	 String Hobby;

	public Mypage(int iD, String name, String nationality, int countryCode, String hobby) {
		ID = iD;
		Name = name;
		Nationality = nationality;
		CountryCode = countryCode;
		Hobby = hobby;
	}
	
	public int getID() {
		return ID;
	}
	public void setID(int iD) {
		ID = iD;
	}
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public String getNationality() {
		return Nationality;
	}
	public void setNationality(String nationality) {
		Nationality = nationality;
	}
	public int getCountryCode() {
		return CountryCode;
	}
	public void setCountryCode(int countyCode) {
		CountryCode = countyCode;
	}
	public String getHobby() {
		return Hobby;
	}
	public void setHobby(String hobby) {
		Hobby = hobby;
	}
}
