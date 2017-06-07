package misc;

import java.time.LocalDateTime;

public class Misc {

	public Misc() {

	}

	public static String writeLog(NiveauxLogs niveau, String message) {

		LocalDateTime timePoint = LocalDateTime.now();

		return timePoint + " [" + niveau + "] " + message;
	}

}
