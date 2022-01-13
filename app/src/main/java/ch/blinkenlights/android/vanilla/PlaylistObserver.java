package ch.blinkenlights.android.vanilla;

/**
 * A class which observes playlist changes and acts upon them.
 */
public interface PlaylistObserver {
	/**
	 * Stop observing playlist changes.
	 */
	void unregister();
}
