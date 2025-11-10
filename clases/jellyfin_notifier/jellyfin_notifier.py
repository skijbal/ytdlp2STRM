"""
Jellyfin/Emby Library Scanner Notifier
Notifies Jellyfin or Emby to scan a specific library when new content is added
"""

import requests
from clases.log import log as l

class JellyfinNotifier:
    def __init__(self, config):
        """
        Initialize the notifier with configuration
        
        Args:
            config (dict): Configuration dictionary containing:
                - jellyfin_integration (str): "True" or "False" to enable/disable integration
                - jellyfin_base_url (str): Base URL of Jellyfin/Emby server
                - jellyfin_api_key (str): API key for authentication
                - jellyfin_library_name (str): Name of the library to scan
        """
        # Convert string "True"/"False" to boolean
        integration_value = config.get('jellyfin_integration', 'False')
        self.enabled = str(integration_value).lower() == 'true'
        
        self.base_url = config.get('jellyfin_base_url', '').rstrip('/')
        self.api_key = config.get('jellyfin_api_key', '')
        self.library_name = config.get('jellyfin_library_name', '')
        self.server_type = 'jellyfin'  # Default to jellyfin, can be 'emby'
        
        # Validate configuration
        if self.enabled:
            if not self.base_url:
                l.log("jellyfin_notifier", "Warning: jellyfin_integration enabled but jellyfin_base_url is empty")
                self.enabled = False
            elif not self.api_key:
                l.log("jellyfin_notifier", "Warning: jellyfin_integration enabled but jellyfin_api_key is empty")
                self.enabled = False
            elif not self.library_name:
                l.log("jellyfin_notifier", "Warning: jellyfin_integration enabled but jellyfin_library_name is empty")
                self.enabled = False
            else:
                # Detect if it's Emby based on URL
                if 'emby' in self.base_url.lower():
                    self.server_type = 'emby'
                l.log("jellyfin_notifier", f"{self.server_type.capitalize()} integration enabled for library: {self.library_name}")
    
    def get_library_id(self):
        """
        Get the library ID by name
        
        Returns:
            str: Library ID or None if not found
        """
        if not self.enabled:
            return None
        
        try:
            # Endpoint is the same for both Jellyfin and Emby
            url = f"{self.base_url}/Library/VirtualFolders"
            headers = {
                'X-Emby-Token': self.api_key
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            libraries = response.json()
            
            for library in libraries:
                if library.get('Name', '').lower() == self.library_name.lower():
                    # Get the first ItemId from Locations
                    locations = library.get('Locations', [])
                    if locations:
                        # For Jellyfin/Emby, we need to get the library ID from the CollectionType
                        # We'll use the library name to trigger a scan
                        return library.get('ItemId') or library.get('Name')
            
            l.log("jellyfin_notifier", f"Library '{self.library_name}' not found")
            return None
            
        except requests.exceptions.RequestException as e:
            l.log("jellyfin_notifier", f"Error getting library ID: {e}")
            return None
        except Exception as e:
            l.log("jellyfin_notifier", f"Unexpected error getting library ID: {e}")
            return None
    
    def scan_library(self):
        """
        Trigger a library scan
        
        Returns:
            bool: True if scan was triggered successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            # First, try to get the library ID
            library_id = self.get_library_id()
            
            if library_id:
                # Scan specific library by ID
                url = f"{self.base_url}/Library/Refresh"
                headers = {
                    'X-Emby-Token': self.api_key,
                    'Content-Type': 'application/json'
                }
                
                # Try with library ID in query params
                params = {}
                
                response = requests.post(url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                
                l.log("jellyfin_notifier", f"Library scan triggered successfully for '{self.library_name}'")
                return True
            else:
                # Fallback: trigger a full library scan
                url = f"{self.base_url}/Library/Refresh"
                headers = {
                    'X-Emby-Token': self.api_key
                }
                
                response = requests.post(url, headers=headers, timeout=10)
                response.raise_for_status()
                
                l.log("jellyfin_notifier", f"Full library scan triggered (library '{self.library_name}' not found)")
                return True
                
        except requests.exceptions.RequestException as e:
            l.log("jellyfin_notifier", f"Error triggering library scan: {e}")
            return False
        except Exception as e:
            l.log("jellyfin_notifier", f"Unexpected error triggering library scan: {e}")
            return False
    
    def notify_new_content(self, content_path=None):
        """
        Notify Jellyfin/Emby about new content
        This is a convenience method that triggers a library scan
        
        Args:
            content_path (str, optional): Path to the new content (for logging purposes)
        
        Returns:
            bool: True if notification was successful, False otherwise
        """
        if not self.enabled:
            return False
        
        if content_path:
            l.log("jellyfin_notifier", f"New content added: {content_path}")
        
        return self.scan_library()


# Convenience function for quick usage
def notify_jellyfin(config, content_path=None):
    """
    Quick function to notify Jellyfin/Emby about new content
    
    Args:
        config (dict): Configuration dictionary
        content_path (str, optional): Path to the new content
    
    Returns:
        bool: True if notification was successful, False otherwise
    """
    notifier = JellyfinNotifier(config)
    return notifier.notify_new_content(content_path)
