//! The phosphor-CRT look: palette, egui visuals, and the shared "terminal
//! chrome" used by every panel.
//!
//! The palette is the classic P1-phosphor green of a 1980s monitor and almost
//! nothing else: bright green for what is alive, pale/dim green for text, and
//! two semantic exceptions -- amber strictly for transitional/warning states
//! and red strictly for failure. A cyan accent used to sit alongside them and
//! was removed on request: three hues across one small window read as
//! decoration, and the point of a terminal is that colour *means* something.
//!
//! Two rules hold the whole look together and are worth keeping:
//!
//! 1. **Everything is monospace and nothing is rounded.** Rounded corners are
//!    the single strongest "modern app" signal; a terminal has none. Every
//!    `Rounding` here is [`egui::Rounding::ZERO`] on purpose.
//! 2. **Only ASCII is ever drawn as text.** Box-drawing and block glyphs would
//!    be the obvious way to build bars and frames, but the font eframe bundles
//!    only guarantees ASCII, and a bar that renders as a row of tofu boxes on
//!    someone else's machine is worse than no bar. Every frame, bar and glyph
//!    that is not plain ASCII is *painted* (see `widgets`), not typed.

use egui::{Color32, FontId, RichText, Rounding, Stroke, Ui};

/// P1 phosphor -- the primary "this is alive" colour.
pub const PHOSPHOR: Color32 = Color32::from_rgb(51, 255, 102);
/// A burnt-in trace of the above, for grid lines and inactive bar segments.
pub const PHOSPHOR_DIM: Color32 = Color32::from_rgb(26, 110, 56);
/// Amber phosphor: transitional states (starting, stopping, warnings).
pub const AMBER: Color32 = Color32::from_rgb(255, 176, 0);
/// Failure only. Never used decoratively, so it always means something.
pub const RED: Color32 = Color32::from_rgb(255, 74, 74);

/// Behind the glass. Not pure black: a real CRT never is, and a hair of green
/// in the void is what makes the phosphor read as *glow* rather than as paint.
pub const BG_VOID: Color32 = Color32::from_rgb(4, 9, 6);
/// Panel interiors -- one step up from the void so a panel reads as a raised
/// surface without needing a drop shadow.
pub const BG_PANEL: Color32 = Color32::from_rgb(8, 17, 12);
/// Panel borders and chart grids.
pub const GRID: Color32 = Color32::from_rgb(20, 46, 32);
/// Body text: phosphor, dimmed to something readable for a paragraph.
pub const TEXT: Color32 = Color32::from_rgb(160, 236, 182);
/// Secondary text (units, hints, timestamps).
pub const TEXT_DIM: Color32 = Color32::from_rgb(92, 146, 108);

/// Height of one scanline cell; also the grid the matrix rain falls on, so the
/// two effects line up instead of beating against each other.
pub const CELL: f32 = 3.0;

/// The type scale. Every label in the window is one of these sizes, all
/// monospace (rule 1 above).
///
/// Hints, units, captions and the small print under a button.
pub const SIZE_CAPTION: f32 = 10.0;
/// Messages, URLs and log lines.
pub const SIZE_BODY: f32 = 11.0;
/// A secondary readout sitting next to a headline one.
pub const SIZE_SUBREADOUT: f32 = 12.0;
/// Counters (streams, peers, swarms).
pub const SIZE_COUNTER: f32 = 13.0;
/// The one headline number: download throughput.
pub const SIZE_READOUT: f32 = 17.0;

/// Monospace text at `size` in `color`.
pub fn mono(text: impl Into<String>, size: f32, color: Color32) -> RichText {
    RichText::new(text)
        .font(FontId::monospace(size))
        .color(color)
}

/// [`SIZE_CAPTION`] text.
pub fn caption(text: impl Into<String>, color: Color32) -> RichText {
    mono(text, SIZE_CAPTION, color)
}

/// [`SIZE_BODY`] text.
pub fn body(text: impl Into<String>, color: Color32) -> RichText {
    mono(text, SIZE_BODY, color)
}

/// Installs the CRT visuals and forces every text style onto the monospace
/// family, so a stray `ui.label` can never break the terminal illusion.
pub fn apply(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::dark();

    visuals.panel_fill = Color32::TRANSPARENT;
    visuals.window_fill = BG_PANEL;
    visuals.extreme_bg_color = BG_VOID;
    visuals.faint_bg_color = BG_PANEL;
    visuals.override_text_color = Some(TEXT);

    visuals.window_rounding = Rounding::ZERO;
    visuals.menu_rounding = Rounding::ZERO;
    visuals.window_stroke = Stroke::new(1.0, GRID);
    visuals.popup_shadow = egui::epaint::Shadow::NONE;
    visuals.window_shadow = egui::epaint::Shadow::NONE;

    visuals.selection.bg_fill = PHOSPHOR.linear_multiply(0.25);
    visuals.selection.stroke = Stroke::new(1.0, PHOSPHOR);
    visuals.hyperlink_color = PHOSPHOR;

    for widget in [
        &mut visuals.widgets.noninteractive,
        &mut visuals.widgets.inactive,
        &mut visuals.widgets.hovered,
        &mut visuals.widgets.active,
        &mut visuals.widgets.open,
    ] {
        widget.rounding = Rounding::ZERO;
        widget.bg_fill = BG_PANEL;
        widget.weak_bg_fill = BG_PANEL;
        widget.bg_stroke = Stroke::new(1.0, GRID);
        widget.fg_stroke = Stroke::new(1.0, TEXT);
    }
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, PHOSPHOR);
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.0, PHOSPHOR);
    visuals.widgets.active.bg_stroke = Stroke::new(1.0, PHOSPHOR);
    visuals.widgets.active.fg_stroke = Stroke::new(1.0, PHOSPHOR);
    ctx.set_visuals(visuals);

    let mut style = (*ctx.style()).clone();
    for (text_style, size) in [
        (egui::TextStyle::Small, 10.0),
        (egui::TextStyle::Body, 12.0),
        (egui::TextStyle::Button, 12.0),
        (egui::TextStyle::Monospace, 12.0),
        (egui::TextStyle::Heading, 15.0),
    ] {
        style
            .text_styles
            .insert(text_style, FontId::monospace(size));
    }
    style.spacing.item_spacing = egui::vec2(8.0, 6.0);
    style.spacing.button_padding = egui::vec2(8.0, 4.0);
    style.spacing.scroll.bar_width = 6.0;
    style.spacing.scroll.floating = false;
    ctx.set_style(style);
}

/// Paints `text` with a soft phosphor bloom around it.
///
/// A CRT does not draw a crisp glyph -- the electron beam excites the phosphor
/// slightly wider than the beam itself. Four offset copies at low alpha is a
/// cheap stand-in that survives at any DPI, and it is what stops bright green
/// text on near-black from looking like flat vector art.
pub fn glow_text(
    painter: &egui::Painter,
    pos: egui::Pos2,
    anchor: egui::Align2,
    text: &str,
    font: FontId,
    color: Color32,
) -> egui::Rect {
    let halo = Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 26);
    for (dx, dy) in [(1.0, 0.0), (-1.0, 0.0), (0.0, 1.0), (0.0, -1.0)] {
        painter.text(pos + egui::vec2(dx, dy), anchor, text, font.clone(), halo);
    }
    painter.text(pos, anchor, text, font, color)
}

/// The HUD corner brackets: four short L-shapes that suggest a frame without
/// closing one. Used on buttons and the header so they read as instrument
/// panels rather than as web cards.
pub fn corner_brackets(painter: &egui::Painter, rect: egui::Rect, len: f32, stroke: Stroke) {
    let corners = [
        (rect.left_top(), 1.0, 1.0),
        (rect.right_top(), -1.0, 1.0),
        (rect.left_bottom(), 1.0, -1.0),
        (rect.right_bottom(), -1.0, -1.0),
    ];
    for (corner, sx, sy) in corners {
        painter.line_segment([corner, corner + egui::vec2(len * sx, 0.0)], stroke);
        painter.line_segment([corner, corner + egui::vec2(0.0, len * sy)], stroke);
    }
}

/// A bordered section whose title sits *in* the top border, the way a boxed
/// TUI panel does (`+- NET I/O ----+`).
///
/// Painted rather than typed for the ASCII reason in the module docs: the
/// border is a stroke, and the title punches a hole in it by drawing its own
/// backdrop over the line.
///
/// `title` is drawn as given, so callers pass it already in capitals -- a
/// constant, or a string rendered once when its data arrived -- rather than
/// having it uppercased and re-formatted on every frame.
pub fn section<R>(ui: &mut Ui, title: &str, accent: Color32, add: impl FnOnce(&mut Ui) -> R) -> R {
    let inner = egui::Frame::none()
        .fill(BG_PANEL)
        .stroke(Stroke::new(1.0, GRID))
        .inner_margin(egui::Margin {
            left: 12.0,
            right: 12.0,
            top: 16.0,
            bottom: 12.0,
        })
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            add(ui)
        });

    let rect = inner.response.rect;
    let galley =
        ui.painter()
            .layout_no_wrap(title.to_owned(), FontId::monospace(SIZE_CAPTION), accent);
    // The gap either side of the title, one monospace cell wide: what the
    // padding spaces around the label used to be.
    let pad = 6.0;
    let pos = egui::pos2(rect.left() + 10.0 + pad, rect.top() - galley.size().y * 0.5);
    ui.painter().rect_filled(
        egui::Rect::from_min_size(
            pos - egui::vec2(pad, 0.0),
            galley.size() + egui::vec2(pad * 2.0, 0.0),
        ),
        Rounding::ZERO,
        BG_PANEL,
    );
    ui.painter().galley(pos, galley, accent);
    corner_brackets(ui.painter(), rect, 6.0, Stroke::new(1.0, accent));

    inner.inner
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The look depends on staying inside a small palette; a stray colour is
    /// the way a themed UI rots. This pins the surviving hues so a "small
    /// tweak" to one constant has to be a deliberate edit here too -- and so
    /// the removed cyan cannot quietly come back as a fourth hue.
    #[test]
    fn palette_is_green_plus_two_semantic_hues() {
        assert_eq!((PHOSPHOR.r(), PHOSPHOR.g(), PHOSPHOR.b()), (51, 255, 102));
        assert_eq!((AMBER.r(), AMBER.g(), AMBER.b()), (255, 176, 0));
        assert_eq!((RED.r(), RED.g(), RED.b()), (255, 74, 74));
        // Every non-semantic colour is a green: text, dim text, grid, panels.
        for c in [TEXT, TEXT_DIM, PHOSPHOR_DIM, GRID, BG_PANEL, BG_VOID] {
            assert!(c.g() >= c.r() && c.g() >= c.b(), "a non-green crept in");
        }
    }

    /// The void must never be pure black -- see `BG_VOID`.
    #[test]
    fn the_void_keeps_a_trace_of_phosphor() {
        assert!(BG_VOID.g() > BG_VOID.r(), "background lost its green cast");
        assert!(BG_PANEL.g() > BG_PANEL.b());
    }
}
