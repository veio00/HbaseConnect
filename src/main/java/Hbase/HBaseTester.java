package Hbase;

import java.io.IOException;

public class HBaseTester {
	public static final String usage = 
		"The arguments is missing - Choose an option below:\n"+
		"add user_rk name email - add a new user\n"+
		"get user_rk - retrieve the user\n"+
		"delete user_rk - remove the user\n"+
		"list - list all users\n";
	
	public static final String r = "\n\n -----> Result here: \n\n";

		public static void main(String[] args) throws IOException{
		//if(args.length == 0){
		//	System.out.println(usage);
		//	System.exit(0);
		//}

		AccessObject ao = new AccessObject();
				
		//if("get".equals(args[0])){
		//	System.out.println(r);
		//	System.out.println("Getting user " + args[1]);
		//	User u = ao.getUser(args[1]);
		//	System.out.println(u);
		//}
		//if("add".equals(args[0])){
		//	System.out.println(r);
		//	System.out.println("Adding user...");
		//	ao.addUser(args[1],args[2],args[3]);
		//	User u = ao.getUser(args[1]);
		//	System.out.println("Successfully added user " + u);
		//}
		//
		//if("delete".equals(args[0])){
		//	System.out.println(r);
		//	System.out.println("Deleting user...");
		//	ao.deleteUser(args[1]);
		//	System.out.println("Successfully deleted user");
		//}
		//
		//if("list".equals(args[0])){
		//	System.out.println(r);
		//	for(User u : ao.getUsers()){
		//		System.out.println("Listing users...");
		//		System.out.println(u);
		//	}
		//}
			ao.getUsers();
			ao.addUser("ana", "ana s.", "ana.s@la.logicalis.com");

	}
}
