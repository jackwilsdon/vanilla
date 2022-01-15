package ch.blinkenlights.android.vanilla;

import android.content.ContentValues;
import android.content.Context;
import android.database.ContentObserver;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.Build;
import android.os.Environment;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Message;
import android.os.Process;
import android.provider.DocumentsContract;
import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.RequiresApi;
import androidx.documentfile.provider.DocumentFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.CRC32;

import ch.blinkenlights.android.medialibrary.LibraryObserver;
import ch.blinkenlights.android.medialibrary.MediaLibrary;

@RequiresApi(Build.VERSION_CODES.R)
public class ScopedPlaylistObserver implements Handler.Callback, PlaylistObserver {
	private static final int MSG_SCAN = 0;
	private static final int MSG_IMPORT = 1;
	private static final int MSG_EXPORT = 2;

	private static final int SYNC_MODE_PURGE = (1 << 2);

	private final Database mDatabase;
	private final Context mContext;
	private final int mSyncMode;
	private final boolean mExportRelativePaths;
	private DocumentFile mSyncFolder;
	private HandlerThread mHandlerThread;
	private Handler mHandler;

	private final ContentObserver mContentObserver = new ContentObserver(null) {
		@Override
		public void onChange(boolean selfChange) {
			onChange(selfChange, null);
		}

		@Override
		public void onChange(boolean selfChange, @Nullable Uri uri) {
			if (uri == null) {
				// We need to scan everything if we don't know what has changed.
				mHandler.sendEmptyMessage(MSG_SCAN);
			} else {
				DocumentFile file = DocumentFile.fromTreeUri(mContext, uri);
				if (file == null || !file.isFile()) {
					return;
				}

				String name = file.getName();
				if (name == null) {
					Log.d("ScopedPlaylistObserver", "received change for file without name: " + uri);
					return;
				}

				if (name.endsWith(".m3u")) {
					mHandler.sendMessage(mHandler.obtainMessage(MSG_IMPORT, file));
				}
			}
		}

		@Override
		public void onChange(boolean selfChange, @Nullable Uri uri, int flags) {
			onChange(selfChange, uri);
		}

		@Override
		public void onChange(boolean selfChange, @NonNull Collection<Uri> uris, int flags) {
			for (Uri uri : uris) {
				onChange(selfChange, uri, flags);
			}
		}
	};

	private final LibraryObserver mLibraryObserver = new LibraryObserver() {
		@Override
		public void onChange(Type type, long id, boolean ongoing) {
			// Ignore non-playlist or ongoing changes (we only want to handle finished changes).
			if (type != Type.PLAYLIST || ongoing) {
				return;
			}

			mHandler.sendMessage(mHandler.obtainMessage(MSG_EXPORT, id));
		}
	};

	public ScopedPlaylistObserver(Context context, String syncFolder, int syncMode, boolean exportRelativePaths) {
		mDatabase = new Database(context);
		mContext = context;
		mSyncMode = syncMode;
		mExportRelativePaths = exportRelativePaths;

		try {
			Uri uri = Uri.parse(syncFolder);
			mSyncFolder = DocumentFile.fromTreeUri(context, uri);
			mContext.getContentResolver().registerContentObserver(uri, false, mContentObserver);
		} catch (IllegalArgumentException exception) {
			Log.e("ScopedPlaylistObserver", "failed to get document file for sync folder: " + syncFolder, exception);
			return;
		}

		// Set up a handler thread and handler so that we can perform playlist operations in a
		// separate thread.
		mHandlerThread = new HandlerThread("ScopedPlaylistObserver", Process.THREAD_PRIORITY_LOWEST);
		mHandlerThread.start();
		mHandler = new Handler(mHandlerThread.getLooper(), this);

		MediaLibrary.registerLibraryObserver(mLibraryObserver);

		// Start a scan of the library.
		mHandler.sendEmptyMessage(MSG_SCAN);
	}

	@Override
	public void unregister() {
		if (mSyncFolder == null) {
			return;
		}

		mContext.getContentResolver().unregisterContentObserver(mContentObserver);

		mHandlerThread.quitSafely();

		MediaLibrary.unregisterLibraryObserver(mLibraryObserver);
	}

	@Override
	public boolean handleMessage(@NonNull Message message) {
		if (message.what == MSG_SCAN) {
			scan();
			return true;
		} else if (message.what == MSG_IMPORT) {
			importPlaylist((DocumentFile) message.obj);
		} else if (message.what == MSG_EXPORT) {
			exportPlaylist((long) message.obj, "m3u");
			return true;
		}

		Log.e("ScopedPlaylistObserver", "handleMessage: unknown message type: " + message.what);

		return false;
	}

	private void scan() {
		Cursor cursor = mDatabase.query(null, null);
		boolean purge = (mSyncMode & SYNC_MODE_PURGE) == SYNC_MODE_PURGE;
		List<String> knownFiles = new ArrayList<>();
		while (cursor.moveToNext()) {
			long id = cursor.getLong(0);
			String name = cursor.getString(1);

			String filename = sanitizeFilename(name + ".m3u");
			DocumentFile file = mSyncFolder.findFile(filename);

			if (Playlist.getPlaylist(mContext, id) == null) {
				// This playlist has been deleted - rename the file if purging
				// is enabled.
				if (purge && file != null) {
					file.renameTo(sanitizeFilename(name + ".backup"));
				}

				// Forget about this playlist.
				mDatabase.delete(Database.ID + " = ?", new String[]{Long.toString(id)});
			} else if (file == null && purge) {
				// This playlist's M3U has been deleted - save a backup of it.
				exportPlaylist(id, "backup");

				// Remove the playlist.
				Playlist.deletePlaylist(mContext, id);
				mDatabase.delete(Database.ID + " = ?", new String[]{Long.toString(id)});
			}

			if (file != null) {
				knownFiles.add(filename);
			}
		}
		cursor.close();

		for (DocumentFile file : mSyncFolder.listFiles()) {
			// Skip non-m3u files and files that we already know about.
			String name = file.getName();
			if (name == null || !name.endsWith(".m3u") || knownFiles.contains(file.getName())) {
				continue;
			}

			// Import files which we don't have playlists for.
			if (Playlist.getPlaylist(mContext, name.substring(name.length() - 4)) == -1) {
				mHandler.sendMessage(mHandler.obtainMessage(MSG_IMPORT, file));
			}
		}
	}

	private void importPlaylist(DocumentFile file) {
		Uri uri = file.getUri();
		long hash = hashFile(uri);
		if (hash == -1) {
			return;
		}

		String name = file.getName();
		if (name == null) {
			Log.e("ScopedPlaylistObserver", "cannot import file without name: " + uri);
			return;
		}

		// If this playlist's hash hasn't changed, we don't need to import it.
		String playlistName = name.substring(name.length() - 4);
		long existingPlaylistId = -1;
		Cursor cursor = mDatabase.query(Database.NAME + " = ?", new String[]{playlistName});
		if (cursor.moveToNext()) {
			// A playlist with this name exists - check if it has changed.
			if (hash == cursor.getLong(2)) {
				Log.d("ScopedPlaylistObserver", "skipping import (hash unchanged): " + uri);
				cursor.close();
				return;
			} else {
				existingPlaylistId = cursor.getLong(0);
			}
		}

		// Stop observing the library so that we don't notify ourselves during import.
		MediaLibrary.unregisterLibraryObserver(mLibraryObserver);

		// If the playlist already exists then this will overwrite it.
		long playlistId = Playlist.createPlaylist(mContext, playlistName);

		// Store the playlist hash if this is a new playlist.
		if (existingPlaylistId == -1) {
			ContentValues contentValues = new ContentValues();
			contentValues.put(Database.ID, playlistId);
			contentValues.put(Database.NAME, playlistName);
			contentValues.put(Database.HASH, hash);
			mDatabase.insert(contentValues);
		}

		Path syncFolderPath = getAbsolutePath(mSyncFolder.getUri());
		if (syncFolderPath == null) {
			Log.d("ScopedPlaylistObserver", "importPlaylist: failed to get sync folder path");
		}

		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(mContext.getContentResolver().openInputStream(uri)))) {
			while (true) {
				String line = bufferedReader.readLine();
				if (line == null) {
					break;
				}

				// Ignore empty lines and comments.
				if (line.isEmpty() || line.startsWith("#")) {
					continue;
				}

				Path path = Paths.get(FileUtils.normalizeDirectorySeparators(line));

				// Convert relative paths to absolute ones.
				if (syncFolderPath != null) {
					path = syncFolderPath.resolve(path).normalize();
				}

				Playlist.addToPlaylist(mContext, playlistId, MediaUtils.buildFileQuery(path.toString(), Song.FILLED_PROJECTION, false));
			}

			// Update our stored ID and hash if this isn't a new playlist.
			if (existingPlaylistId != -1) {
				ContentValues contentValues = new ContentValues();
				contentValues.put(Database.ID, playlistId);
				contentValues.put(Database.HASH, hash);
				mDatabase.update(contentValues, Database.ID + " = ?", new String[]{Long.toString(existingPlaylistId)});
			}
		} catch (IOException exception) {
			Log.e("ScopedPlaylistObserver", "failed to import playlist: " + uri, exception);
		}

		MediaLibrary.registerLibraryObserver(mLibraryObserver);
	}

	private void exportPlaylist(long id, String extension) {
		String name = Playlist.getPlaylist(mContext, id);
		if (name == null) {
			Log.e("ScopedPlaylistObserver", "exportPlaylist: no such playlist: " + id);
			return;
		}

		String filename = sanitizeFilename(name + "." + extension);
		DocumentFile file = mSyncFolder.findFile(filename);
		if (file == null) {
			file = mSyncFolder.createFile("audio/x-mpegurl", filename);
			if (file == null) {
				Log.e("ScopedPlaylistObserver", "exportPlaylist: failed to create file: " + filename);
				return;
			}
		}

		Path syncFolderPath = getAbsolutePath(mSyncFolder.getUri());
		if (syncFolderPath == null) {
			Log.d("ScopedPlaylistObserver", "exportPlaylist: failed to get sync folder path");
		}

		try (OutputStream outputStream = mContext.getContentResolver().openOutputStream(file.getUri())) {
			Cursor cursor = MediaUtils.buildPlaylistQuery(id, new String[]{MediaLibrary.SongColumns.PATH}).runQuery(mContext);
			PrintWriter printWriter = new PrintWriter(outputStream);

			while (cursor.moveToNext()) {
				String path = cursor.getString(0);
				if (mExportRelativePaths && syncFolderPath != null) {
					path = syncFolderPath.relativize(Paths.get(path)).toString();
				}
				printWriter.println(path);
			}

			cursor.close();
			printWriter.flush();
		} catch (IOException exception) {
			Log.e("ScopedPlaylistObserver", "exportPlaylist: failed to export playlist: " + filename, exception);
		}
	}

	/**
	 * Get the absolute filesystem path of a file.
	 */
	private Path getAbsolutePath(Uri uri) {
		if (!uri.getAuthority().equals("com.android.externalstorage.documents")) {
			return null;
		}

		String id = DocumentsContract.getDocumentId(uri);
		String[] pieces = id.split(":");
		if (!pieces[0].equals("primary")) {
			return null;
		}

		return Paths.get(Environment.getExternalStorageDirectory() + "/" + pieces[1]);
	}

	private String sanitizeFilename(String filename) {
		return filename.replaceAll("/", "_");
	}

	/**
	 * Calculate the CRC-32 of a file.
	 */
	private long hashFile(Uri uri) {
		long hash = -1;
		try (InputStream inputStream = mContext.getContentResolver().openInputStream(uri)) {
			byte[] buffer = new byte[4096];
			CRC32 crc = new CRC32();
			while (inputStream.read(buffer) != -1) {
				crc.update(buffer);
			}
			hash = Math.abs(crc.getValue());
		} catch (IOException exception) {
			Log.e("ScopedPlaylistObserver", "failed to hash file: " + uri, exception);
		}
		return hash;
	}

	private static final class Database extends SQLiteOpenHelper {
		private static final String TABLE_NAME = "playlist_metadata";
		private static final String ID = "_id";
		private static final String NAME = "name";
		private static final String HASH = "hash";

		private Database(Context context) {
			super(context, "playlist_observer.db", null, 1);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(
					"CREATE TABLE " + TABLE_NAME + " (" +
							ID + " INTEGER PRIMARY KEY," +
							HASH + " INTEGER NOT NULL," +
							NAME + " TEXT NOT NULL" +
							")"
			);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			// We only have one version so far.
		}

		public long insert(ContentValues contentValues) {
			return getWritableDatabase().insert(TABLE_NAME, null, contentValues);
		}

		public Cursor query(String selection, String[] selectionArgs) {
			return getReadableDatabase().query(TABLE_NAME, new String[]{ID, HASH, NAME}, selection, selectionArgs, null, null, null);
		}

		public int update(ContentValues contentValues, String whereClause, String[] whereArgs) {
			return getWritableDatabase().update(TABLE_NAME, contentValues, whereClause, whereArgs);
		}

		public int delete(String whereClause, String[] whereArgs) {
			return getWritableDatabase().delete(TABLE_NAME, whereClause, whereArgs);
		}
	}
}
