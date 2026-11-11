; Inno Setup script for the Windows installer. Build it with
; scripts/windows/build_installer.ps1, which passes AppVersion and StageDir and
; sets the output folder and file name.

#ifndef AppVersion
  #error AppVersion must be defined
#endif
#ifndef StageDir
  #error StageDir must be defined
#endif

[Setup]
AppId={{6F0E4C1B-6E2A-4F5B-9C1D-2B7A9E3F8D41}
AppName=zBase
AppVersion={#AppVersion}
AppPublisher=zBase
AppPublisherURL=https://github.com/cameroncooper/zbase
DefaultDirName={localappdata}\Programs\zBase
DefaultGroupName=zBase
DisableProgramGroupPage=yes
; Per-user install: no administrator prompt.
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
SetupIconFile={#SourcePath}\..\..\assets\windows\zBase.ico
UninstallDisplayIcon={app}\zbase.exe
UninstallDisplayName=zBase
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
CloseApplications=yes

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "{#StageDir}\zbase.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#StageDir}\*.dll"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#StageDir}\licenses\*"; DestDir: "{app}\licenses"; Flags: ignoreversion recursesubdirs

; The app user model ID matches the one zbase.exe sets; Windows needs it to
; show toasts and to group the taskbar button with the shortcut.
[Icons]
Name: "{autoprograms}\zBase"; Filename: "{app}\zbase.exe"; AppUserModelID: "com.zbase.app"
Name: "{autodesktop}\zBase"; Filename: "{app}\zbase.exe"; Tasks: desktopicon; AppUserModelID: "com.zbase.app"

; zbase.exe registers its app ID for notifications on launch; remove it on uninstall.
[Registry]
Root: HKCU; Subkey: "Software\Classes\AppUserModelId\com.zbase.app"; Flags: uninsdeletekey dontcreatekey

[Run]
Filename: "{app}\zbase.exe"; Description: "{cm:LaunchProgram,zBase}"; Flags: nowait postinstall skipifsilent
