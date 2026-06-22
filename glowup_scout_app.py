import customtkinter as ctk

ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

app = ctk.CTk()
app.title("GlowUp Scout")
app.geometry("700x500")

title = ctk.CTkLabel(app, text="GlowUp Product Scout", font=("Arial", 28, "bold"))
title.pack(pady=30)

label = ctk.CTkLabel(app, text="Ricerca prodotto singolo da EAN")
label.pack(pady=10)

ean_entry = ctk.CTkEntry(app, placeholder_text="Inserisci EAN", width=300)
ean_entry.pack(pady=10)

button = ctk.CTkButton(app, text="Analizza EAN")
button.pack(pady=10)

result = ctk.CTkLabel(app, text="Qui compariranno i risultati Amazon", wraplength=600)
result.pack(pady=30)

app.mainloop()
