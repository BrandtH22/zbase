; Inno Setup script for the Windows installer. Build it with
; scripts/windows/build_installer.ps1, which passes AppVersion and StageDir and
; sets the output folder and file name.

#ifndef AppVersion
  #error AppVersion must be defined
#endif
#ifndef StageDir
  #error StageDir must be defined
#endif
; zbase.exe sets the same app user model ID (APP_ID in its notifications
; service); toasts and taskbar grouping need the two to match.
#define AppUserModelId "com.zbase.app"

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

[Icons]
Name: "{autoprograms}\zBase"; Filename: "{app}\zbase.exe"; AppUserModelID: "{#AppUserModelId}"
Name: "{autodesktop}\zBase"; Filename: "{app}\zbase.exe"; Tasks: desktopicon; AppUserModelID: "{#AppUserModelId}"

; zbase.exe registers its app ID on launch, and Windows keeps per-app
; notification settings once a toast shows; remove both on uninstall.
[Registry]
Root: HKCU; Subkey: "Software\Classes\AppUserModelId\{#AppUserModelId}"; Flags: uninsdeletekey dontcreatekey
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Notifications\Settings\{#AppUserModelId}"; Flags: uninsdeletekey dontcreatekey

[Run]
Filename: "{app}\zbase.exe"; Description: "{cm:LaunchProgram,zBase}"; Flags: nowait postinstall skipifsilent
