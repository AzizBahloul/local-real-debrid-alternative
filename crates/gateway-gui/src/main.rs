//! NovaStream's desktop launcher, dressed as the terminal it is standing in
//! for.
//!
//! The visual language is a phosphor CRT: digital rain behind the glass,
//! scanlines and a refresh sweep over it, and instrument panels in between.
//! The reason it is worth the code is that this window's whole job is to
//! answer "is the gateway alive and is data actually moving?" from across a
//! room, and a live graph plus a breathing lamp answer that in a glance where
//! a column of numbers does not.
//!
//! Layer discipline, since it is easy to break:
//!   background layer -> the rain (painted first, before any panel)
//!   panel layer      -> every widget (panel frames are transparent, so the
//!                       rain shows through the margins)
//!   foreground layer -> the CRT overlay, on top of everything
//!   tooltip layer    -> the cold-boot cover, on top of that
//!
//! The FX live in [`fx`], the palette and chrome in [`theme`], and the
//! instruments in [`widgets`]. The window's state, actions and panels are in
//! [`app`]; the command line is [`cli`]; this file is only the frame loop.

mod addon_address;
mod app;
mod cli;
mod fx;
mod health;
mod http;
mod job;
mod server_process;
mod service;
mod text;
mod theme;
mod tray;
mod widgets;

use std::path::PathBuf;

use app::GatewayApp;
use cli::Mode;

impl eframe::App for GatewayApp {
    /// The void behind every panel. Panels are transparent so the rain painted
    /// into the background layer stays visible through them.
    fn clear_color(&self, _visuals: &egui::Visuals) -> [f32; 4] {
        theme::BG_VOID.to_normalized_gamma_f32()
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_background_work();

        // The close button is a "put it away", not a "shut it down" -- unless
        // the quit action set `quitting` first, in which case this same event
        // means exactly what it says.
        if !self.quitting() && ctx.input(|i| i.viewport().close_requested()) {
            self.hand_over_to_tray();
        }

        let status = self.status();
        let screen = ctx.screen_rect();
        let time = ctx.input(|i| i.time);
        // Clamped so a stall cannot teleport the rain, but loosely enough that
        // the throttled unfocused frame rate still falls at real speed.
        let dt = ctx.input(|i| i.stable_dt).min(0.25);

        self.paint_rain(ctx, screen, dt, time);

        let bare = egui::Frame::none().inner_margin(egui::Margin::symmetric(14.0, 10.0));
        egui::TopBottomPanel::top("hud")
            .frame(bare)
            .show(ctx, |ui| self.hud(ui, status, time));
        egui::TopBottomPanel::bottom("tty")
            .frame(bare)
            .resizable(true)
            .default_height(180.0)
            .min_height(74.0)
            .show(ctx, |ui| self.tty_panel(ui, time));
        egui::CentralPanel::default()
            .frame(bare)
            .show(ctx, |ui| self.dashboard(ui, status, time));

        self.paint_crt(ctx, screen);
        self.paint_boot(ctx, screen, time);

        self.request_next_frame(ctx);
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.stop_child_blocking();
    }
}

/// The app icon (cropped from `data/bourguiba.jpg`), embedded directly into
/// the binary so the window/taskbar icon is correct regardless of where the
/// binary was launched from -- no runtime dependency on an installed path.
fn app_icon() -> egui::IconData {
    eframe::icon_data::from_png_bytes(include_bytes!("../assets/icon-256.png"))
        .expect("bundled icon-256.png is a valid PNG")
}

fn main() -> eframe::Result<()> {
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("streaming-gateway-gui"));
    match cli::parse_mode(std::env::args().skip(1)) {
        Mode::Help => {
            print!("{}", cli::USAGE);
            return Ok(());
        }
        Mode::Tray => std::process::exit(tray::run_tray_daemon(exe)),
        mode @ (Mode::EnableAlwaysOn | Mode::DisableAlwaysOn | Mode::Status) => {
            std::process::exit(cli::run_service_command(mode, &exe))
        }
        Mode::Window => {}
    }

    // One face at a time: a tray icon left by an earlier close would sit next
    // to the window that is about to open, and clicking it would try to open a
    // second one.
    tray::stop_running_tray();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            // Taller than it is wide, like a terminal: the dashboard is a
            // stack of instruments and the TTY needs room under it.
            .with_inner_size([600.0, 880.0])
            .with_min_inner_size([460.0, 560.0])
            .with_icon(app_icon()),
        ..Default::default()
    };
    eframe::run_native(
        "NovaStream",
        options,
        Box::new(|cc| {
            theme::apply(&cc.egui_ctx);
            Ok(Box::new(GatewayApp::default()))
        }),
    )
}
